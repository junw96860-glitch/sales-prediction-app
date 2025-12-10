import streamlit as st
import uuid
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import math
import json
import os
from functools import lru_cache
from typing import Dict, List, Any, Optional
import base64
from io import BytesIO
from PIL import Image
import matplotlib.pyplot as plt


class DataManager:
    """数据管理类，负责数据的加载、保存和兼容性处理"""
    
    @staticmethod
    def save_data_to_json(data: Any, filename: str) -> bool:
        """保存数据到JSON文件"""
        try:
            if isinstance(data, pd.DataFrame):
                df_copy = data.copy()
                for col in df_copy.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
                    elif df_copy[col].dtype == 'object':  # 检查object类型列，可能包含date对象
                        # 检查是否包含date/datetime对象
                        for i, val in enumerate(df_copy[col]):
                            if isinstance(val, (datetime, date)):
                                df_copy.iat[i, df_copy.columns.get_loc(col)] = val.strftime('%Y-%m-%d')
                json_data = df_copy.to_dict('records')
            else:
                json_data = data
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            st.error(f"保存JSON文件失败: {str(e)}")
            return False

    @staticmethod
    def load_data_from_json(filename: str) -> pd.DataFrame:
        """从JSON文件加载数据"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if not content:  # 检查文件是否为空
                        st.info(f"文件 {filename} 为空，返回空DataFrame")
                        return pd.DataFrame()
                    
                    # 尝试修复常见的JSON格式问题
                    content = content.replace(',]', ']').replace(',}', '}')
                    # 检查是否以[开头且以]结尾，或以{开头且以}结尾
                    content_stripped = content.strip()
                    if content_stripped and not (content_stripped.startswith('[') and content_stripped.endswith(']')) and not (content_stripped.startswith('{') and content_stripped.endswith('}')):
                        st.error(f"文件 {filename} 不是有效的JSON格式")
                        return pd.DataFrame()
                    
                    json_data = json.loads(content)
                    
                if json_data:
                    df = pd.DataFrame(json_data)
                    # 只转换已知的日期列，避免将其他object类型误转换
                    for col in ['交付日期', '开始日期', '结束日期', '收入日期', '支出日期']:
                        if col in df.columns:
                            # 先尝试转换为datetime，如果失败则跳过
                            try:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                            except:
                                # 如果转换失败，保持原值
                                pass
                    return df
            return pd.DataFrame()
        except json.JSONDecodeError as e:
            st.error(f"JSON格式错误: {str(e)}")
            st.error(f"错误位置: 第{e.lineno}行，第{e.colno}列")
            # 尝试从错误位置附近显示内容以帮助调试
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if e.lineno <= len(lines):
                        error_line = lines[e.lineno-1].strip()
                        st.error(f"错误行内容: {error_line}")
            except:
                pass
            return pd.DataFrame()
        except Exception as e:
            st.error(f"加载JSON文件失败: {str(e)}")
            return pd.DataFrame()

    @staticmethod
    def ensure_columns_compatibility(df: pd.DataFrame) -> pd.DataFrame:
        """确保数据框包含必需的列"""
        required_columns = ['首付款比例', '次付款比例', '质保金比例']
        for col in required_columns:
            if col not in df.columns:
                df[col] = {'首付款比例': 50, '次付款比例': 40, '质保金比例': 10}[col]
        return df


class IncomeManager:
    """收入管理类，处理收入预测和现金流计算"""
    
    def __init__(self, df: pd.DataFrame = None, material_ratios: Dict = None):
        self.data = df if df is not None else pd.DataFrame()
        self.material_ratios = material_ratios or {
            '光谱设备/服务': 0.30, '配液设备': 0.35, '自动化项目': 0.40
        }

    def generate_summary(self) -> pd.DataFrame:
        """生成收入摘要数据"""
        if self.data.empty: 
            return pd.DataFrame()
        
        summary_data = []
        self.data['交付日期'] = pd.to_datetime(self.data['交付日期'])
        self.data['交付季度'] = self.data['交付日期'].dt.quarter
        self.data['交付年份'] = self.data['交付日期'].dt.year
        self.data['季度'] = self.data['交付年份'].astype(str) + '-Q' + self.data['交付季度'].astype(str)
        
        # 按季度聚合数据
        quarterly = self.data.groupby('季度').agg(
            金额=('纠偏后收入', 'sum'),
            项目数=('项目名称', 'count'),
            平均衰减=('时间衰减因子', 'mean'),
            合同总额=('合同金额', 'sum')
        ).reset_index()
        quarterly = quarterly.sort_values('季度')
        quarterly['累计金额'] = quarterly['金额'].cumsum()
        quarterly['累计占比'] = quarterly['累计金额'] / quarterly['金额'].sum() * 100 if quarterly['金额'].sum() > 0 else 0
        
        for _, row in quarterly.iterrows():
            summary_data.append({
                '类别': '季度收入', '项目': row['季度'], '金额': round(row['金额'], 2),
                '项目数': row['项目数'], '平均衰减': round(row['平均衰减'], 4),
                '累计占比': round(row['累计占比'], 1), '合同总额': round(row['合同总额'], 2)
            })
        
        # 按业务线聚合数据
        business = self.data.groupby('业务线').agg(
            金额=('纠偏后收入', 'sum'),
            项目数=('项目名称', 'count'),
            合同总额=('合同金额', 'sum')
        ).reset_index()
        business['贡献率'] = business['金额'] / business['金额'].sum() * 100 if business['金额'].sum() > 0 else 0
        business = business.sort_values('金额', ascending=False)
        
        for _, row in business.iterrows():
            summary_data.append({
                '类别': '业务线', '项目': row['业务线'], '金额': round(row['金额'], 2),
                '项目数': row['项目数'], '贡献率': round(row['贡献率'], 1),
                '合同总额': round(row['合同总额'], 2)
            })
        
        # 计算核心指标
        total_revenue = self.data['预期收入'].sum()
        total_adjusted_revenue = self.data['纠偏后收入'].sum()
        total_contract = self.data['合同金额'].sum()
        avg_decay = self.data['时间衰减因子'].mean() if not self.data.empty else 0
        conversion_rate = total_adjusted_revenue / total_contract * 100 if total_contract > 0 else 0
        
        summary_data.extend([
            {'类别': '核心指标', '项目': '总预期收入', '金额': round(total_revenue, 2), '项目数': len(self.data), '贡献率': None, '合同总额': round(total_contract, 2)},
            {'类别': '核心指标', '项目': '总纠偏后收入', '金额': round(total_adjusted_revenue, 2), '项目数': len(self.data), '贡献率': None, '合同总额': round(total_contract, 2)},
            {'类别': '核心指标', '项目': '平均时间衰减', '金额': round(avg_decay, 4), '项目数': None, '贡献率': None},
            {'类别': '核心指标', '项目': '整体转化率', '金额': round(conversion_rate, 1), '项目数': None, '贡献率': None}
        ])
        
        return pd.DataFrame(summary_data)

    def generate_cash_flow_data(self) -> pd.DataFrame:
        """生成现金流数据"""
        cash_flow_data = []
        for _, project in self.data.iterrows():
            delivery_date = pd.to_datetime(project['交付日期'])
            first_payment_ratio = project.get('首付款比例', 50) / 100.0
            second_payment_ratio = project.get('次付款比例', 40) / 100.0
            final_payment_ratio = project.get('质保金比例', 10) / 100.0
            total_ratio = first_payment_ratio + second_payment_ratio + final_payment_ratio
            if abs(total_ratio - 1.0) > 0.001:
                st.warning(f"项目 {project['项目名称']} 的付款比例总和不是100%，当前总和: {total_ratio*100:.1f}%")
            
            # 首付款
            first_payment_date = delivery_date
            first_payment_amount = project['纠偏后收入'] * first_payment_ratio
            cash_flow_data.append({
                '项目名称': project['项目名称'], '现金流类型': '首付款', '支付日期': first_payment_date,
                '支付月份': f"{first_payment_date.year}-{first_payment_date.month:02d}", '金额': round(first_payment_amount, 2),
                '业务线': project['业务线'], '付款比例': f"{project.get('首付款比例', 50)}%"
            })
            
            # 次付款
            second_payment_date = delivery_date + pd.DateOffset(months=1)
            second_payment_amount = project['纠偏后收入'] * second_payment_ratio
            cash_flow_data.append({
                '项目名称': project['项目名称'], '现金流类型': '次付款', '支付日期': second_payment_date,
                '支付月份': f"{second_payment_date.year}-{second_payment_date.month:02d}", '金额': round(second_payment_amount, 2),
                '业务线': project['业务线'], '付款比例': f"{project.get('次付款比例', 40)}%"
            })
            
            # 质保金
            final_payment_date = delivery_date + pd.DateOffset(years=1)
            final_payment_amount = project['纠偏后收入'] * final_payment_ratio
            cash_flow_data.append({
                '项目名称': project['项目名称'], '现金流类型': '质保金', '支付日期': final_payment_date,
                '支付月份': f"{final_payment_date.year}-{final_payment_date.month:02d}", '金额': round(final_payment_amount, 2),
                '业务线': project['业务线'], '付款比例': f"{project.get('质保金比例', 10)}%"
            })
        return pd.DataFrame(cash_flow_data)

    def generate_material_cost_data(self) -> pd.DataFrame:
        """生成物料成本数据"""
        material_cost_data = []
        for _, project in self.data.iterrows():
            business_line = project['业务线']
            material_ratio = self.material_ratios.get(business_line, 
                {'光谱设备/服务': 0.30, '配液设备': 0.35, '自动化项目': 0.40}.get(business_line, 0.30))
            material_cost = project['纠偏后收入'] * material_ratio
            delivery_date = pd.to_datetime(project['交付日期'])
            material_payment_date = delivery_date - pd.DateOffset(months=1)
            material_cost_data.append({
                '项目名称': project['项目名称'], '业务线': project['业务线'],
                '物料支出比例': material_ratio * 100, '物料成本': round(material_cost, 2),
                '支出月份': f"{material_payment_date.year}-{material_payment_date.month:02d}",
                '支出日期': material_payment_date
            })
        return pd.DataFrame(material_cost_data)


class CostManager:
    """成本管理基类"""
    
    def __init__(self, df: pd.DataFrame = None):
        self.data = df if df is not None else pd.DataFrame()

    def generate_cost_data(self) -> pd.DataFrame:
        """生成成本数据 - 子类需实现"""
        raise NotImplementedError("子类必须实现generate_cost_data方法")


class LaborCostManager(CostManager):
    """人工成本管理类"""
    
    def generate_cost_data(self) -> pd.DataFrame:
        """生成人工成本数据"""
        if self.data.empty: 
            return pd.DataFrame()
        
        labor_data = self.data.copy()
        labor_data['开始日期'] = pd.to_datetime(labor_data['开始日期'])
        labor_data['结束日期'] = pd.to_datetime(labor_data['结束日期'])
        monthly_costs = []
        for _, row in labor_data.iterrows():
            start_date = row['开始日期']
            end_date = row['结束日期']
            current_date = start_date.replace(day=1)
            while current_date <= end_date:
                month_end = current_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
                if month_end > end_date: 
                    month_end = end_date
                days_in_month = (current_date + pd.DateOffset(months=1) - current_date).days
                actual_end = min(month_end, end_date)
                days_for_cost = (actual_end - current_date).days + 1
                monthly_amount = row['月度成本'] * (days_for_cost / days_in_month)
                monthly_costs.append({
                    '成本类型': row['成本类型'], '人员/部门': row['人员/部门'],
                    '成本金额': round(monthly_amount, 2),
                    '支出月份': f"{current_date.year}-{current_date.month:02d}",
                    '开始日期': row['开始日期'], '结束日期': row['结束日期']
                })
                current_date = current_date + pd.DateOffset(months=1)
        return pd.DataFrame(monthly_costs)


class AdminCostManager(CostManager):
    """行政费用管理类"""
    
    def generate_cost_data(self) -> pd.DataFrame:
        """生成行政费用数据"""
        if self.data.empty: 
            return pd.DataFrame()
        
        admin_cost_data = []
        for _, row in self.data.iterrows():
            cost_type = row['费用类型']
            monthly_cost = row['月度成本']
            start_date = pd.to_datetime(row['开始日期'])
            end_date = pd.to_datetime(row['结束日期'])
            payment_frequency = row['付款频率']
            
            if payment_frequency == '月度':
                current_date = start_date.replace(day=1)
                while current_date <= end_date:
                    admin_cost_data.append({
                        '费用类型': cost_type, '费用项目': row['费用项目'],
                        '月度成本': round(monthly_cost, 2),
                        '支出月份': f"{current_date.year}-{current_date.month:02d}",
                        '支出日期': current_date, '付款频率': payment_frequency
                    })
                    current_date = current_date + pd.DateOffset(months=1)
            elif payment_frequency == '季度':
                current_date = start_date.replace(day=1)
                while current_date <= end_date:
                    admin_cost_data.append({
                        '费用类型': cost_type, '费用项目': row['费用项目'],
                        '月度成本': round(monthly_cost * 3, 2),
                        '支出月份': f"{current_date.year}-{current_date.month:02d}",
                        '支出日期': current_date, '付款频率': payment_frequency
                    })
                    current_date = current_date + pd.DateOffset(months=3)
            elif payment_frequency == '年度':
                current_date = start_date.replace(day=1)
                admin_cost_data.append({
                    '费用类型': cost_type, '费用项目': row['费用项目'],
                    '月度成本': round(monthly_cost * 12, 2),
                    '支出月份': f"{current_date.year}-{current_date.month:02d}",
                    '支出日期': current_date, '付款频率': payment_frequency
                })
        return pd.DataFrame(admin_cost_data)


class ExportManager:
    """导出管理类，负责将数据导出为各种格式"""
    
    @staticmethod
    def export_to_excel(data_dict: Dict[str, pd.DataFrame], filename: str) -> BytesIO:
        """将多个数据框导出到Excel文件"""
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        output.seek(0)
        return output

    @staticmethod
    def export_to_csv(df: pd.DataFrame, filename: str) -> BytesIO:
        """将数据框导出到CSV文件"""
        output = BytesIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)
        return output

    @staticmethod
    def export_visualization(fig, filename: str) -> BytesIO:
        """导出可视化图表为图片"""
        try:
            img_buffer = BytesIO()
            fig.write_image(img_buffer, format='png')
            img_buffer.seek(0)
            return img_buffer
        except Exception as e:
            # 如果导出失败，显示错误信息但不中断程序
            if "kaleido" in str(e):
                st.error("图表导出需要安装kaleido包: pip install kaleido")
            else:
                st.error(f"导出图表时发生错误: {str(e)}")
            return BytesIO()  # 返回空的BytesIO对象



def generate_template_data() -> Dict[str, pd.DataFrame]:
    """生成各类数据模板"""
    templates = {}
    
    # 收入预测模板
    templates['income'] = pd.DataFrame({
        '项目名称': ['示例项目1', '示例项目2'],
        '交付日期': ['2026-03-15', '2026-06-20'],
        '合同金额': [100.0, 150.0],
        '保守成单率': [50, 80],
        '业务线': ['光谱设备/服务', '自动化项目'],
        '首付款比例': [50, 30],
        '次付款比例': [40, 50],
        '质保金比例': [10, 20]
    })
    
    # 人工成本模板
    templates['labor'] = pd.DataFrame({
        '成本类型': ['销售费用', '制造费用', '研发费用', '管理费用'],
        '人员/部门': ['销售团队', '生产团队', '研发团队', '管理层'],
        '月度成本': [50.0, 80.0, 120.0, 30.0],
        '开始日期': ['2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01'],
        '结束日期': ['2025-12-31', '2025-12-31', '2025-12-31', '2025-12-31']
    })
    
    # 行政费用模板 - 更新费用类型
    templates['admin'] = pd.DataFrame({
        '费用类型': ['房租费用', '水电费用', '办公用品', '差旅费用', '研发支出', '营销支出', '售前支出', '财务税费'],
        '费用项目': ['总部大楼', '水电费', '办公用品采购', '差旅费', '研发设备', '市场推广', '售前支持', '税费缴纳'],
        '月度成本': [10.0, 2.0, 1.0, 3.0, 5.0, 4.0, 2.5, 1.5],
        '开始日期': ['2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01'],
        '结束日期': ['2025-12-31', '2025-12-31', '2025-12-31', '2025-12-31', '2025-12-31', '2025-12-31', '2025-12-31', '2025-12-31'],
        '付款频率': ['月度', '月度', '季度', '月度', '月度', '月度', '月度', '季度']
    })
    
    return templates


def create_visualization_charts(data_manager, material_ratios) -> Dict[str, go.Figure]:
    """创建所有可视化图表"""
    charts = {}
    
    if not data_manager['income'].data.empty:
        # 季度收入分布
        summary_df = data_manager['income'].generate_summary()
        quarterly_data = summary_df[summary_df['类别'] == '季度收入']
        if not quarterly_data.empty:
            quarterly_data = quarterly_data.copy()
            quarterly_data['项目_中文'] = quarterly_data['项目'].apply(lambda x: x.replace('-Q', '年Q'))
            fig_q = go.Figure()
            fig_q.add_trace(go.Bar(x=quarterly_data['项目_中文'], y=quarterly_data['金额'], name='纠偏后收入', marker_color='#1a2a6c'))
            fig_q.add_trace(go.Scatter(x=quarterly_data['项目_中文'], y=quarterly_data['累计占比'], name='累计占比', yaxis='y2', mode='lines+markers', line=dict(color='#ff2e2e', width=3), marker=dict(size=8)))
            fig_q.update_layout(title='季度收入分布与累计占比', xaxis_title='季度', yaxis_title='纠偏后收入 (万元)', yaxis2=dict(title='累计占比 (%)', overlaying='y', side='right'), hovermode='x unified', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            charts['quarterly_income'] = fig_q
        
        # 业务线收入贡献
        business_data = summary_df[summary_df['类别'] == '业务线']
        if not business_data.empty:
            fig_b = px.pie(business_data, values='金额', names='项目', title='业务线收入贡献', hole=0.3, color_discrete_sequence=px.colors.qualitative.Set3)
            fig_b.update_traces(textposition='inside', textinfo='percent+label')
            charts['business_income'] = fig_b
            
            fig_b2 = go.Figure()
            fig_b2.add_trace(go.Bar(x=business_data['项目'], y=business_data['金额'], name='纠偏后收入', marker_color='#1a2a6c'))
            fig_b2.add_trace(go.Bar(x=business_data['项目'], y=business_data['合同总额'], name='合同总额', marker_color='#83c9ff'))
            fig_b2.update_layout(barmode='group', title='业务线收入对比', xaxis_title='业务线', yaxis_title='金额 (万元)', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            charts['business_income_comparison'] = fig_b2
        
        # 时间衰减趋势
        decay_data = data_manager['income'].data.copy()
        decay_data['交付年月'] = pd.to_datetime(decay_data['交付日期']).dt.strftime('%Y-%m')
        fig_adj = px.scatter(decay_data, x='预期收入', y='纠偏后收入', size='纠偏后收入', color='业务线', hover_name='项目名称', hover_data=['合同金额', '保守成单率', '时间衰减因子'], title='纠偏后收入 vs 预期收入')
        max_val = max(decay_data['预期收入'].max(), decay_data['纠偏后收入'].max())
        fig_adj.add_trace(go.Scatter(x=[0, max_val], y=[0, max_val], mode='lines', name='y=x参考线', line=dict(color='red', dash='dash')))
        fig_adj.update_layout(xaxis_title='预期收入', yaxis_title='纠偏后收入', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
        charts['decay_trend'] = fig_adj
        
        # 时间衰减曲线
        months = np.arange(0, 25)
        decay_values = np.exp(-0.0315 * months)
        fig_curve = go.Figure()
        fig_curve.add_trace(go.Scatter(x=months, y=decay_values, mode='lines+markers', name='λ=0.0315', line=dict(color='#1a2a6c', width=3)))
        fig_curve.update_layout(title='时间衰减曲线', xaxis_title='月份数', yaxis_title='衰减因子', yaxis_range=[0, 1.05], hovermode='x unified', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
        charts['decay_curve'] = fig_curve
    
    # 物料支出分析
    if not data_manager['income'].data.empty:
        material_cost_df = data_manager['income'].generate_material_cost_data()
        if not material_cost_df.empty:
            business_material_summary = material_cost_df.groupby('业务线').agg({'物料成本': 'sum', '物料支出比例': 'mean'}).reset_index()
            fig_material = px.pie(business_material_summary, values='物料成本', names='业务线', title='业务线物料支出分布', hole=0.3, color_discrete_sequence=px.colors.qualitative.Set3)
            fig_material.update_traces(textposition='inside', textinfo='percent+label')
            charts['material_distribution'] = fig_material
            
            monthly_material_cost = material_cost_df.groupby('支出月份')['物料成本'].sum().reset_index()
            monthly_material_cost['支出月份'] = pd.to_datetime(monthly_material_cost['支出月份'])
            monthly_material_cost = monthly_material_cost.sort_values('支出月份')
            monthly_material_cost['支出月份_中文'] = monthly_material_cost['支出月份'].apply(lambda x: f"{x.year}年{x.month}月")
            fig_monthly_material = px.line(monthly_material_cost, x='支出月份_中文', y='物料成本', title='月度物料支出趋势', markers=True)
            fig_monthly_material.update_layout(xaxis_title='月份', yaxis_title='物料成本 (万元)', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            charts['monthly_material_trend'] = fig_monthly_material
    
    # 现金流分析
    if not data_manager['income'].data.empty:
        cash_flow_df = data_manager['income'].generate_cash_flow_data()
        if not cash_flow_df.empty:
            monthly_cash_flow = cash_flow_df.groupby('支付月份').agg({'金额': 'sum'}).reset_index()
            monthly_cash_flow['支付月份'] = pd.to_datetime(monthly_cash_flow['支付月份'])
            monthly_cash_flow = monthly_cash_flow.sort_values('支付月份')
            monthly_cash_flow['支付月份_中文'] = monthly_cash_flow['支付月份'].apply(lambda x: f"{x.year}年{x.month}月")
            
            fig_cf = go.Figure()
            for cash_type in cash_flow_df['现金流类型'].unique():
                type_data = cash_flow_df[cash_flow_df['现金流类型'] == cash_type]
                monthly_type = type_data.groupby('支付月份').agg({'金额': 'sum'}).reset_index()
                monthly_type['支付月份'] = pd.to_datetime(monthly_type['支付月份'])
                monthly_type = monthly_type.sort_values('支付月份')
                monthly_type['支付月份_中文'] = monthly_type['支付月份'].apply(lambda x: f"{x.year}年{x.month}月")
                fig_cf.add_trace(go.Bar(x=monthly_type['支付月份'], y=monthly_type['金额'], name=cash_type, text=monthly_type['金额'], textposition='auto'))
            fig_cf.update_layout(title='月度现金流分布', xaxis_title='月份', yaxis_title='金额 (万元)', barmode='stack', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            charts['cash_flow_distribution'] = fig_cf
    
    # 全面预算汇总
    if not data_manager['income'].data.empty:
        income_monthly = data_manager['income'].data.copy()
        income_monthly['交付月份'] = pd.to_datetime(income_monthly['交付日期']).dt.to_period('M')
        income_summary = income_monthly.groupby('交付月份')['纠偏后收入'].sum().reset_index()
        income_summary['月份'] = income_summary['交付月份'].astype(str)
        
        material_monthly = data_manager['income'].generate_material_cost_data()
        if not material_monthly.empty: 
            material_summary = material_monthly.groupby('支出月份')['物料成本'].sum().reset_index()
        else: 
            material_summary = pd.DataFrame(columns=['支出月份', '物料成本'])
        
        labor_monthly = data_manager['labor'].generate_cost_data()
        if not labor_monthly.empty: 
            labor_summary = labor_monthly.groupby('支出月份')['成本金额'].sum().reset_index()
        else: 
            labor_summary = pd.DataFrame(columns=['支出月份', '成本金额'])
        
        admin_monthly = data_manager['admin'].generate_cost_data()
        if not admin_monthly.empty: 
            admin_summary = admin_monthly.groupby('支出月份')['月度成本'].sum().reset_index()
        else: 
            admin_summary = pd.DataFrame(columns=['支出月份', '月度成本'])
        
        all_months = set()
        if not income_summary.empty: 
            all_months.update(income_summary['月份'])
        if not material_summary.empty: 
            all_months.update(material_summary['支出月份'])
        if not labor_summary.empty: 
            all_months.update(labor_summary['支出月份'])
        if not admin_summary.empty: 
            all_months.update(admin_summary['支出月份'])
        
        months_list = sorted(list(all_months))
        budget_summary = pd.DataFrame({'月份': months_list})
        
        if not income_summary.empty:
            budget_summary = budget_summary.merge(income_summary[['月份', '纠偏后收入']], on='月份', how='left').fillna(0)
        else: 
            budget_summary['纠偏后收入'] = 0
        
        if not material_summary.empty:
            budget_summary = budget_summary.merge(material_summary[['支出月份', '物料成本']], left_on='月份', right_on='支出月份', how='left').fillna(0)
            budget_summary.drop(columns=['支出月份'], inplace=True)
        else: 
            budget_summary['物料成本'] = 0
        
        if not labor_summary.empty:
            budget_summary = budget_summary.merge(labor_summary[['支出月份', '成本金额']], left_on='月份', right_on='支出月份', how='left').fillna(0)
            budget_summary.drop(columns=['支出月份'], inplace=True)
        else: 
            budget_summary['成本金额'] = 0
        
        if not admin_summary.empty:
            budget_summary = budget_summary.merge(admin_summary[['支出月份', '月度成本']], left_on='月份', right_on='支出月份', how='left').fillna(0)
            budget_summary.drop(columns=['支出月份'], inplace=True)
        else: 
            budget_summary['月度成本'] = 0
        
        budget_summary['总收入'] = budget_summary['纠偏后收入']
        budget_summary['总支出'] = budget_summary['物料成本'] + budget_summary['成本金额'] + budget_summary['月度成本']
        budget_summary['毛利润'] = budget_summary['总收入'] - budget_summary['总支出']
        budget_summary['毛利率'] = np.where(budget_summary['总收入'] > 0, budget_summary['毛利润'] / budget_summary['总收入'] * 100, 0)
        budget_summary['月份_dt'] = pd.to_datetime(budget_summary['月份'])
        budget_summary = budget_summary.sort_values('月份_dt')
        budget_summary = budget_summary.drop('月份_dt', axis=1)
        budget_summary['月份_中文'] = pd.to_datetime(budget_summary['月份']).apply(lambda x: f"{x.year}年{x.month}月")
        budget_summary = budget_summary.rename(columns={'月份': '月份_英文'})
        budget_summary = budget_summary.rename(columns={'月份_中文': '月份'})
        
        fig_budget = go.Figure()
        fig_budget.add_trace(go.Bar(x=budget_summary['月份'], y=budget_summary['总收入'], name='总收入', marker_color='#1a2a6c'))
        fig_budget.add_trace(go.Bar(x=budget_summary['月份'], y=budget_summary['物料成本'], name='物料成本', marker_color='#ff6b6b'))
        fig_budget.add_trace(go.Bar(x=budget_summary['月份'], y=budget_summary['成本金额'], name='人工成本', marker_color='#4ecdc4'))
        fig_budget.add_trace(go.Bar(x=budget_summary['月份'], y=budget_summary['月度成本'], name='行政费用', marker_color='#f7b731'))
        fig_budget.update_layout(title='月度收入与支出对比', xaxis_title='月份', yaxis_title='金额 (万元)', barmode='group', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
        charts['budget_comparison'] = fig_budget
        
        fig_margin = go.Figure()
        fig_margin.add_trace(go.Scatter(x=budget_summary['月份'], y=budget_summary['毛利率'], mode='lines+markers', name='毛利率', line=dict(color='#1a2a6c', width=3), marker=dict(size=8)))
        fig_margin.update_layout(title='月度毛利率趋势', xaxis_title='月份', yaxis_title='毛利率 (%)', yaxis_range=[-100, 100], plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
        charts['margin_trend'] = fig_margin
    
    return charts
def create_executive_dashboard_charts(data_manager):
    """创建老板视角的经营概览图表"""
    charts = {}
    
    if not data_manager['income'].data.empty:
        # 获取预算汇总数据
        income_monthly = data_manager['income'].data.copy()
        income_monthly['交付月份'] = pd.to_datetime(income_monthly['交付日期']).dt.to_period('M')
        income_summary = income_monthly.groupby('交付月份')['纠偏后收入'].sum().reset_index()
        income_summary['月份'] = income_summary['交付月份'].astype(str)
        
        material_monthly = data_manager['income'].generate_material_cost_data()
        if not material_monthly.empty: 
            material_summary = material_monthly.groupby('支出月份')['物料成本'].sum().reset_index()
        else: 
            material_summary = pd.DataFrame(columns=['支出月份', '物料成本'])
        
        labor_monthly = data_manager['labor'].generate_cost_data()
        if not labor_monthly.empty: 
            labor_summary = labor_monthly.groupby('支出月份')['成本金额'].sum().reset_index()
        else: 
            labor_summary = pd.DataFrame(columns=['支出月份', '成本金额'])
        
        admin_monthly = data_manager['admin'].generate_cost_data()
        if not admin_monthly.empty: 
            admin_summary = admin_monthly.groupby('支出月份')['月度成本'].sum().reset_index()
        else: 
            admin_summary = pd.DataFrame(columns=['支出月份', '月度成本'])
        
        # 构建月度汇总数据
        all_months = set()
        if not income_summary.empty: all_months.update(income_summary['月份'])
        if not material_summary.empty: all_months.update(material_summary['支出月份'])
        if not labor_summary.empty: all_months.update(labor_summary['支出月份'])
        if not admin_summary.empty: all_months.update(admin_summary['支出月份'])
        
        months_list = sorted(list(all_months))
        budget_summary = pd.DataFrame({'月份': months_list})
        
        if not income_summary.empty:
            budget_summary = budget_summary.merge(income_summary[['月份', '纠偏后收入']], on='月份', how='left').fillna(0)
        else: 
            budget_summary['纠偏后收入'] = 0
        
        if not material_summary.empty:
            budget_summary = budget_summary.merge(material_summary[['支出月份', '物料成本']], left_on='月份', right_on='支出月份', how='left').fillna(0)
            budget_summary.drop(columns=['支出月份'], inplace=True)
        else: 
            budget_summary['物料成本'] = 0
        
        if not labor_summary.empty:
            budget_summary = budget_summary.merge(labor_summary[['支出月份', '成本金额']], left_on='月份', right_on='支出月份', how='left').fillna(0)
            budget_summary.drop(columns=['支出月份'], inplace=True)
        else: 
            budget_summary['成本金额'] = 0
        
        if not admin_summary.empty:
            budget_summary = budget_summary.merge(admin_summary[['支出月份', '月度成本']], left_on='月份', right_on='支出月份', how='left').fillna(0)
            budget_summary.drop(columns=['支出月份'], inplace=True)
        else: 
            budget_summary['月度成本'] = 0
        
        budget_summary['总收入'] = budget_summary['纠偏后收入']
        budget_summary['总支出'] = budget_summary['物料成本'] + budget_summary['成本金额'] + budget_summary['月度成本']
        budget_summary['毛利润'] = budget_summary['总收入'] - budget_summary['总支出']
        budget_summary['毛利率'] = np.where(budget_summary['总收入'] > 0, budget_summary['毛利润'] / budget_summary['总收入'] * 100, 0)
        budget_summary['月份_dt'] = pd.to_datetime(budget_summary['月份'])
        budget_summary = budget_summary.sort_values('月份_dt')
        budget_summary = budget_summary.drop('月份_dt', axis=1)
        budget_summary['月份_中文'] = pd.to_datetime(budget_summary['月份']).apply(lambda x: f"{x.year}年{x.month}月")
        
        # 1. 经营概览仪表板 - 整体关键指标
        total_revenue = budget_summary['总收入'].sum()
        total_expense = budget_summary['总支出'].sum()
        total_profit = budget_summary['毛利润'].sum()
        avg_margin = budget_summary['毛利率'].mean() if len(budget_summary) > 0 else 0
        
        fig_overview = go.Figure()
        fig_overview.add_trace(go.Indicator(
            mode="number+gauge+delta",
            value=total_revenue,
            domain={'x': [0, 1], 'y': [0.6, 1]},
            title={'text': "总收入"},
            gauge={
                'shape': "bullet",
                'axis': {'range': [None, max(total_revenue * 1.2, 1)]},
                'bar': {'color': "#1a2a6c"},
                'steps': [
                    {'range': [0, total_revenue * 0.5], 'color': "lightgray"},
                    {'range': [total_revenue * 0.5, total_revenue], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 2},
                    'thickness': 0.75,
                    'value': total_revenue
                }
            }
        ))
        
        fig_overview.add_trace(go.Indicator(
            mode="number+gauge+delta",
            value=total_profit,
            domain={'x': [0, 1], 'y': [0.3, 0.5]},
            title={'text': "总毛利润"},
            gauge={
                'shape': "bullet",
                'axis': {'range': [None, max(total_profit * 1.2, 1)]},
                'bar': {'color': "#4ecdc4"},
                'steps': [
                    {'range': [0, total_profit * 0.5], 'color': "lightgray"},
                    {'range': [total_profit * 0.5, total_profit], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 2},
                    'thickness': 0.75,
                    'value': total_profit
                }
            }
        ))
        
        fig_overview.add_trace(go.Indicator(
            mode="gauge+number+delta",
            value=avg_margin,
            domain={'x': [0, 1], 'y': [0, 0.2]},
            title={'text': "平均毛利率"},
            gauge={
                'axis': {'range': [0, 100]},
                'bar': {'color': "#f7b731"},
                'steps': [
                    {'range': [0, 30], 'color': "lightcoral"},
                    {'range': [30, 50], 'color': "orange"},
                    {'range': [50, 100], 'color': "lightgreen"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': avg_margin
                }
            }
        ))
        
        fig_overview.update_layout(height=400, title="经营概览仪表板")
        charts['executive_overview'] = fig_overview
        
        # 2. 收入与支出对比 - 清晰展示盈利状况
        fig_profit = go.Figure()
        fig_profit.add_trace(go.Bar(x=budget_summary['月份_中文'], y=budget_summary['总收入'], name='总收入', marker_color='#1a2a6c'))
        fig_profit.add_trace(go.Bar(x=budget_summary['月份_中文'], y=-budget_summary['总支出'], name='总支出', marker_color='#ff6b6b'))
        fig_profit.add_trace(go.Scatter(x=budget_summary['月份_中文'], y=budget_summary['毛利润'], mode='lines+markers', name='毛利润', yaxis='y2', line=dict(color='#4ecdc4', width=3), marker=dict(size=8)))
        fig_profit.update_layout(
            title='收入支出对比及盈利情况',
            xaxis_title='月份',
            yaxis=dict(title='金额 (万元)', side='left'),
            yaxis2=dict(title='毛利润 (万元)', side='right', overlaying='y'),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        charts['profit_analysis'] = fig_profit
        
        # 3. 业务线贡献热力图 - 展示各业务线表现
        summary_df = data_manager['income'].generate_summary()
        business_data = summary_df[summary_df['类别'] == '业务线']
        if not business_data.empty:
            fig_heatmap = go.Figure(data=go.Heatmap(
                z=[[row['金额'] for _, row in business_data.iterrows()]],
                x=business_data['项目'],
                y=['业务线收入贡献'],
                colorscale='Blues',
                text=[[f"{row['金额']:.1f}万<br>{row['贡献率']:.1f}%" for _, row in business_data.iterrows()]],
                texttemplate="%{text}",
                textfont={"size": 12}
            ))
            fig_heatmap.update_layout(title='业务线收入贡献热力图', height=200)
            charts['business_heatmap'] = fig_heatmap
        
        # 4. 成本结构饼图 - 展示支出构成
        total_material = budget_summary['物料成本'].sum()
        total_labor = budget_summary['成本金额'].sum()
        total_admin = budget_summary['月度成本'].sum()
        
        cost_labels = ['物料成本', '人工成本', '行政费用']
        cost_values = [total_material, total_labor, total_admin]
        
        # 过滤掉零值以避免图表错误
        filtered_data = [(label, value) for label, value in zip(cost_labels, cost_values) if value > 0]
        if filtered_data:
            labels, values = zip(*filtered_data)
            fig_cost = px.pie(
                values=values, 
                names=labels, 
                title='总成本结构',
                hole=0.3,
                color_discrete_sequence=px.colors.sequential.Plasma_r
            )
            fig_cost.update_traces(textposition='inside', textinfo='percent+label')
            charts['cost_structure'] = fig_cost
    
    return charts

def main():
    """主函数"""
    # 设置页面配置
    st.set_page_config(
        page_title="全面预算管理系统", 
        page_icon="📊", 
        layout="wide", 
        initial_sidebar_state="expanded"
    )

    # 页面样式
    st.markdown("""
    <style>
        body { background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .main { background: rgba(255, 255, 255, 0.95); border-radius: 15px; padding: 20px; box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1); }
        .sidebar .sidebar-content { background: linear-gradient(135deg, #1a2a6c 0%, #2a5298 100%); color: white; }
        .stButton>button { color: #ffffff; background: linear-gradient(135deg, #1a2a6c 0%, #2a5298 100%); border-radius: 8px; border: none; padding: 10px 20px; font-weight: bold; transition: all 0.3s ease; }
        .stButton>button:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2); }
        .stMetric { background: white; padding: 15px; border-radius: 10px; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08); border-left: 4px solid #1a2a6c; }
        .stSelectbox>div>div, .stNumberInput>div>div, .stTextInput>div>div { border-radius: 8px; border: 1px solid #ddd; }
        .stDownloadButton>button { background: linear-gradient(135deg, #2a9d8f 0%, #264653 100%); color: white; border-radius: 8px; border: none; padding: 10px 20px; font-weight: bold; }
        .stTabs>div>div { border-bottom: 2px solid #e0e0e0; }
        .stTabs>div>div>button { font-size: 16px; font-weight: 500; padding: 12px 20px; border-radius: 8px 8px 0 0; }
        .stTabs>div>div>button[aria-selected="true"] { background: linear-gradient(135deg, #1a2a6c 0%, #2a5298 100%); color: white; border: 1px solid #1a2a6c; }
        .stExpander { border-radius: 10px; border: 1px solid #e0e0e0; }
        .stExpander>summary { background: #f8f9fa; padding: 10px; border-radius: 10px 10px 0 0; font-weight: bold; }
        h1, h2, h3 { color: #1a2a6c; }
        .css-1aumxhk { background: linear-gradient(135deg, #1a2a6c 0%, #2a5298 100%) !important; }
        .stDataFrame { border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden; }
        .stDataFrame>div>table { border-collapse: collapse; }
        .stDataFrame>div>table th { background: linear-gradient(135deg, #1a2a6c 0%, #2a5298 100%); color: white; font-weight: bold; }
        .stDataFrame>div>table td { border: 1px solid #e0e0e0; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<h1 style='text-align: center; color: #1a2a6c;'>📊 全面预算管理系统</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #666;'>基于研发思维的严谨预算管理模型</p>", unsafe_allow_html=True)

    # 初始化会话状态
    if 'data_manager' not in st.session_state:
        income_df = DataManager.load_data_from_json('income_budget.json')
        income_df = DataManager.ensure_columns_compatibility(income_df)
        st.session_state.data_manager = {
            'income': IncomeManager(income_df),
            'labor': LaborCostManager(DataManager.load_data_from_json('labor_budget.json')),
            'admin': AdminCostManager(DataManager.load_data_from_json('admin_budget.json')),
            'occasional': {
                'occasional_income': DataManager.load_data_from_json('occasional_income.json'),  # 修改这里
                'occasional_expense': DataManager.load_data_from_json('occasional_expense.json')  # 修改这里
            }
        }
    

    if 'material_ratios' not in st.session_state:
        st.session_state.material_ratios = {
            '光谱设备/服务': 0.30, '配液设备': 0.35, '自动化项目': 0.40
        }

    if 'selected_page' not in st.session_state:
        st.session_state.selected_page = "收入预测"

    if 'current_cash_balance' not in st.session_state:
        cash_balance_file = 'cash_balance.json'
        if os.path.exists(cash_balance_file):
            try:
                with open(cash_balance_file, 'r', encoding='utf-8') as f:
                    cash_data = json.load(f)
                    st.session_state.current_cash_balance = float(cash_data.get('balance', 0.0))
            except Exception as e:
                st.session_state.current_cash_balance = 0.0  # 加载失败则默认为0
        else:
            st.session_state.current_cash_balance = 0.0

    # 导航菜单
    with st.sidebar:
        st.header("导航菜单")
        nav_options = [
            ("💰 收入预测", "收入预测"),
            ("💼 成本管理", "成本管理"),
            ("💸 现金流分析", "现金流分析"),
            ("📋 全面预算汇总", "全面预算汇总"),
            ("⚙️ 系统配置", "系统配置")
        ]
        for icon, page in nav_options:
            if st.button(f"{icon} {page}", key=page):
                st.session_state.selected_page = page

        # 导出功能
        st.markdown("---")
        st.header("导出功能")
        if st.button("📊 导出经营概览报告"):
            charts = create_executive_dashboard_charts(st.session_state.data_manager)
            for chart_name, chart_fig in charts.items():
                img_buffer = ExportManager.export_visualization(chart_fig, f"{chart_name}.png")
                st.download_button(
                    label=f"下载 {chart_name} 报告",
                    data=img_buffer,
                    file_name=f"{chart_name}.png",
                    mime="image/png"
                )

        if st.button("📄 导出数据报表"):
            data_dict = {
                '收入预测': st.session_state.data_manager['income'].data,
                '人工成本': st.session_state.data_manager['labor'].data,
                '行政费用': st.session_state.data_manager['admin'].data
            }
            excel_buffer = ExportManager.export_to_excel(data_dict, "预算数据汇总.xlsx")
            st.download_button(
                label="下载Excel报表",
                data=excel_buffer,
                file_name="预算数据汇总.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    
    # 页面内容
    if st.session_state.selected_page == "系统配置":
        st.header("⚙️ 系统配置")
        decay_lambda = st.number_input("时间衰减系数 (λ)", min_value=0.01, max_value=0.1, value=0.0315, step=0.0001, format="%.4f", help="行业基准值0.0315，值越大表示时间风险越高")
        base_date = st.date_input("预测基准日期", value=datetime(2025, 12, 8), help="计算时间衰减的起点")
        
        st.markdown("---")
        st.subheader("💼 物料支出比例设置")
        for business_line in ['光谱设备/服务', '配液设备', '自动化项目']:
            ratio = st.session_state.material_ratios.get(business_line, 0.30)
            st.session_state.material_ratios[business_line] = st.number_input(
                f"{business_line}物料支出比例 (%)",
                min_value=0.0, max_value=100.0, value=ratio * 100, step=0.1, format="%.1f",
                help=f"{business_line}项目物料成本占收入的比例"
            ) / 100.0
        
        st.markdown("---")
        st.subheader("💰 现金余额设置")
        
        # 使用 session_state 中的值作为默认值
        current_balance = st.number_input(
            "当前现金余额 (万元)",
            min_value=0.0,
            value=float(st.session_state.current_cash_balance),
            step=1.0,
            help="当前可用现金余额",
            key="cash_balance_input"  # 建议加 key 避免警告
        )
        
        # 更新 session state（用户输入时自动同步）
        st.session_state.current_cash_balance = current_balance
        
        # 保存按钮（带唯一 key）
        if st.button("💾 保存现金余额", type="secondary", key="save_cash_balance"):
            cash_balance_file = 'cash_balance.json'
            cash_data = {'balance': st.session_state.current_cash_balance}
            try:
                with open(cash_balance_file, 'w', encoding='utf-8') as f:
                    json.dump(cash_data, f, ensure_ascii=False, indent=2)
                st.success(f"✅ 现金余额已保存为: {st.session_state.current_cash_balance:.2f} 万元")
            except Exception as e:
                st.error(f"❌ 保存失败: {str(e)}")

        
        st.markdown("---")
        st.subheader("💾 数据管理")
        if st.button("💾 保存当前数据到JSON"):
            save_success = True
            data_files = {'income': 'income_budget.json', 'labor': 'labor_budget.json', 'admin': 'admin_budget.json'}
            for key, filename in data_files.items():
                if key == 'income':
                    data = st.session_state.data_manager[key].data
                elif key == 'labor':
                    data = st.session_state.data_manager[key].data
                elif key == 'admin':
                    data = st.session_state.data_manager[key].data
                if not DataManager.save_data_to_json(data, filename):
                    save_success = False
            if save_success:
                st.success("所有预算数据已成功保存为JSON文件！")
            else:
                st.error("部分数据保存失败")
        
        if st.button("🔄 刷新并重新计算", type="primary"):
            st.session_state.data_manager['income'] = IncomeManager(
                st.session_state.data_manager['income'].data,
                st.session_state.material_ratios
            )
            st.success("配置已更新，数据已重新计算！")

    elif st.session_state.selected_page == "收入预测":
        st.header("➕ 新增销售项目")
        col1, col2 = st.columns(2)
        with col1:
            project_name = st.text_input("项目名称", placeholder="例如：合全flow研发四通道拉曼")
            delivery_date = st.date_input("预计交付日期", key="delivery_date")
            contract_amount = st.number_input("合同金额 (万元)", min_value=0.0, value=100.0, step=1.0)
        with col2:
            business_line = st.selectbox("业务线", ["光谱设备/服务", "配液设备", "自动化项目", "其他"])
            close_rate = st.slider("保守成单率 (%)", min_value=0, max_value=100, value=50, step=1)
            manual_adjusted_income = st.number_input(
                "纠偏后收入 (万元)",
                min_value=0.0,
                value=round(contract_amount * 0.5 * math.exp(-0.0315 * 0), 2),
                step=0.01,
                help="直接输入调整后的收入金额"
            )
            st.subheader("付款比例设置")
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                first_payment_ratio = st.number_input("首付款比例 (%)", min_value=0, max_value=100, value=50, step=1)
            with col_b:
                second_payment_ratio = st.number_input("次付款比例 (%)", min_value=0, max_value=100, value=40, step=1)
            with col_c:
                final_payment_ratio = st.number_input("质保金比例 (%)", min_value=0, max_value=100, value=10, step=1)
            total_ratio = first_payment_ratio + second_payment_ratio + final_payment_ratio
            st.caption(f"付款比例总和: {total_ratio}%")
            if total_ratio != 100:
                st.warning(f"付款比例总和不是100%，当前总和: {total_ratio}%")
            st.caption("提示：取销售提供区间的下限值，例如50%-80%取50%")
    
        if st.button("计算并添加项目", type="primary"):
            if not project_name:
                st.error("项目名称不能为空")
            elif total_ratio != 100:
                st.error("付款比例总和必须为100%")
            else:
                base_datetime = datetime.combine(datetime(2025, 12, 8), datetime.min.time())
                delivery_datetime = datetime.combine(delivery_date, datetime.min.time())
                month_diff = (delivery_datetime.year - base_datetime.year) * 12 + (delivery_datetime.month - base_datetime.month)
                if month_diff < 0:
                    month_diff = 0
                time_decay = math.exp(-0.0315 * month_diff)
                adjusted_rate = (close_rate / 100) * time_decay
                expected_revenue = contract_amount * (close_rate / 100) * time_decay
                adjusted_revenue = manual_adjusted_income
                new_project = {
                    'ID': str(uuid.uuid4()),  # ✅ 添加唯一ID
                    '项目名称': project_name,
                    '交付日期': delivery_date,
                    '合同金额': round(contract_amount, 2),
                    '保守成单率': f"{close_rate}%",
                    '业务线': business_line,
                    '时间衰减因子': round(time_decay, 4),
                    '调整后成单率': f"{round(adjusted_rate * 100, 2)}%",
                    '预期收入': round(expected_revenue, 2),
                    '纠偏后收入': round(adjusted_revenue, 2),
                    '首付款比例': first_payment_ratio,
                    '次付款比例': second_payment_ratio,
                    '质保金比例': final_payment_ratio,
                    '交付月份': f"{delivery_date.year}-{delivery_date.month:02d}",
                    '月份数': month_diff
                }
                new_df = pd.DataFrame([new_project])
                if st.session_state.data_manager['income'].data.empty:
                    st.session_state.data_manager['income'].data = new_df.copy()
                else:
                    st.session_state.data_manager['income'].data = pd.concat(
                        [st.session_state.data_manager['income'].data, new_df], ignore_index=True
                    )
                DataManager.save_data_to_json(st.session_state.data_manager['income'].data, 'income_budget.json')
                st.success(
                    f"项目 '{project_name}' 已成功添加！预期收入: {expected_revenue:.2f}万元，纠偏后收入: {adjusted_revenue:.2f}万元"
                )
    
        st.subheader("📥 收入预测数据导入")
        income_template_df = generate_template_data()['income']
        # 确保模板包含 ID 列（可选，但兼容）
        if 'ID' not in income_template_df.columns:
            income_template_df['ID'] = ""
        income_template_csv = income_template_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="下载收入预测导入模板",
            data=income_template_csv,
            file_name="收入预测导入模板.csv",
            mime="text/csv"
        )
        income_uploaded_file = st.file_uploader("上传收入预测数据 (CSV/Excel)", type=['csv', 'xlsx', 'xls'], key="income_upload")
        if income_uploaded_file is not None:
            if st.button("导入收入预测数据", type="primary", key="import_income"):
                try:
                    if income_uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(income_uploaded_file)
                    else:
                        df = pd.read_excel(income_uploaded_file)
    
                    required_columns = ['项目名称', '交付日期', '合同金额', '保守成单率', '业务线']
                    missing_columns = [col for col in required_columns if col not in df.columns]
                    if missing_columns:
                        st.error(f"文件缺少必要列: {', '.join(missing_columns)}")
                    else:
                        df['交付日期'] = pd.to_datetime(df['交付日期']).dt.date
                        base_datetime = datetime.combine(datetime(2025, 12, 8), datetime.min.time())
                        for idx, row in df.iterrows():
                            delivery_datetime = datetime.combine(row['交付日期'], datetime.min.time())
                            month_diff = (delivery_datetime.year - base_datetime.year) * 12 + (delivery_datetime.month - base_datetime.month)
                            if month_diff < 0:
                                month_diff = 0
                            time_decay = math.exp(-0.0315 * month_diff)
                            close_rate_val = float(str(row['保守成单率']).replace('%', ''))
                            expected_revenue = row['合同金额'] * (close_rate_val / 100) * time_decay
                            df.loc[idx, '时间衰减因子'] = round(time_decay, 4)
                            df.loc[idx, '调整后成单率'] = f"{round(close_rate_val * time_decay, 2)}%"
                            df.loc[idx, '预期收入'] = round(expected_revenue, 2)
                            df.loc[idx, '纠偏后收入'] = round(expected_revenue, 2)
                            df.loc[idx, '交付月份'] = f"{row['交付日期'].year}-{row['交付日期'].month:02d}"
                            df.loc[idx, '月份数'] = month_diff
    
                        # ✅ 补充缺失的 ID
                        if 'ID' not in df.columns:
                            df['ID'] = [str(uuid.uuid4()) for _ in range(len(df))]
                        else:
                            df['ID'] = df['ID'].apply(lambda x: x if pd.notna(x) and x != "" else str(uuid.uuid4()))
    
                        df = DataManager.ensure_columns_compatibility(df)
                        if st.session_state.data_manager['income'].data.empty:
                            st.session_state.data_manager['income'].data = df.copy()
                        else:
                            st.session_state.data_manager['income'].data = pd.concat(
                                [st.session_state.data_manager['income'].data, df], ignore_index=True
                            )
                        DataManager.save_data_to_json(st.session_state.data_manager['income'].data, 'income_budget.json')
                        st.success(f"成功导入 {len(df)} 个收入预测项目！")
                except Exception as e:
                    st.error(f"导入收入预测数据时出错: {str(e)}")
    
        # ==================== 项目明细编辑区（唯一一处） ====================
        st.header("📋 项目预测明细")
    
        full_data = st.session_state.data_manager['income'].data.copy()
        if full_data.empty:
            st.info("暂无项目数据，请先新增或导入项目。")
            total_revenue_all = 0.0
            total_adjusted_revenue_all = 0.0
            total_contract_all = 0.0
        else:
            # ✅ 确保有 ID 列（兼容旧数据）
            if 'ID' not in full_data.columns:
                full_data['ID'] = [str(uuid.uuid4()) for _ in range(len(full_data))]
            full_data = DataManager.ensure_columns_compatibility(full_data)
    
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                business_filter = st.selectbox(
                    "业务线筛选",
                    options=["全部"] + sorted(full_data['业务线'].dropna().unique().tolist()),
                    index=0
                )
            with col2:
                month_filter = st.selectbox(
                    "月份筛选",
                    options=["全部"] + sorted(full_data['交付月份'].dropna().unique().tolist()),
                    index=0
                )
            with col3:
                sort_by = st.selectbox(
                    "排序字段",
                    ["交付月份", "合同金额", "纠偏后收入", "时间衰减因子", "业务线"]
                )
            with col4:
                sort_order = st.selectbox("排序方式", ["降序", "升序"])
    
            # 应用筛选
            filtered_df = full_data.copy()
            if business_filter != "全部":
                filtered_df = filtered_df[filtered_df['业务线'] == business_filter]
            if month_filter != "全部":
                filtered_df = filtered_df[filtered_df['交付月份'] == month_filter]
    
            # 排序（注意：不 reset_index，保留原始 index）
            ascending = (sort_order == "升序")
            filtered_df = filtered_df.sort_values(by=sort_by, ascending=ascending)
    
            # 准备显示列（✅ 包含 ID 用于匹配，但前端隐藏）
            display_cols = [
                'ID', '项目名称', '交付月份', '合同金额', '保守成单率',
                '时间衰减因子', '调整后成单率', '预期收入', '纠偏后收入',
                '首付款比例', '次付款比例', '质保金比例', '业务线'
            ]
            display_df = filtered_df[display_cols].copy()
            display_df['删除'] = False
    
            st.subheader("项目信息编辑")
            edited_df = st.data_editor(
                display_df.style.format({
                    '合同金额': '{:.2f}',
                    '时间衰减因子': '{:.4f}',
                    '预期收入': '{:.2f}',
                    '纠偏后收入': '{:.2f}',
                    '首付款比例': '{:.0f}%',
                    '次付款比例': '{:.0f}%',
                    '质保金比例': '{:.0f}%'
                }),
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "ID": None,  # 👈 隐藏 ID 列
                    "纠偏后收入": st.column_config.NumberColumn(
                        "纠偏后收入", help="直接输入调整后的收入金额", min_value=0.0, step=0.01, default=0.0
                    ),
                    "首付款比例": st.column_config.NumberColumn(
                        "首付款比例", help="首付款占总收入的百分比", min_value=0, max_value=100, step=1, default=50
                    ),
                    "次付款比例": st.column_config.NumberColumn(
                        "次付款比例", help="次付款占总收入的百分比", min_value=0, max_value=100, step=1, default=40
                    ),
                    "质保金比例": st.column_config.NumberColumn(
                        "质保金比例", help="质保金占总收入的百分比", min_value=0, max_value=100, step=1, default=10
                    ),
                    "删除": st.column_config.CheckboxColumn("删除", default=False)
                },
                key="filtered_project_editor"
            )
    
            # 处理删除
            if '删除' in edited_df.columns:
                rows_to_delete = edited_df[edited_df['删除'] == True]
                if not rows_to_delete.empty:
                    if st.button(f"🗑️ 删除 {len(rows_to_delete)} 个选中的项目", type="secondary"):
                        ids_to_delete = rows_to_delete['ID'].tolist()
                        mask = full_data['ID'].isin(ids_to_delete)
                        st.session_state.data_manager['income'].data = full_data[~mask].reset_index(drop=True)
                        DataManager.save_data_to_json(st.session_state.data_manager['income'].data, 'income_budget.json')
                        st.success(f"已删除 {len(rows_to_delete)} 个项目！")
                        st.rerun()
    
            # 处理编辑（排除删除列）
            edited_no_del = edited_df.drop(columns=['删除']) if '删除' in edited_df.columns else edited_df
            original_no_del = display_df.drop(columns=['删除']) if '删除' in display_df.columns else display_df
            if not edited_no_del.equals(original_no_del):
                total_ratios = edited_no_del['首付款比例'] + edited_no_del['次付款比例'] + edited_no_del['质保金比例']
                invalid_rows = edited_no_del[total_ratios != 100]
                if not invalid_rows.empty:
                    st.warning(f"以下项目的付款比例总和不是100%: {invalid_rows['项目名称'].tolist()}")
    
                # ✅ 核心修复：通过 ID 更新原始数据
                income_data = st.session_state.data_manager['income'].data
                for _, row in edited_no_del.iterrows():
                    project_id = row['ID']
                    mask = income_data['ID'] == project_id
                    if mask.any():
                        idx = income_data[mask].index[0]
                        income_data.loc[idx, '纠偏后收入'] = round(row['纠偏后收入'], 2)
                        income_data.loc[idx, '首付款比例'] = row['首付款比例']
                        income_data.loc[idx, '次付款比例'] = row['次付款比例']
                        income_data.loc[idx, '质保金比例'] = row['质保金比例']
    
                st.session_state.data_manager['income'].data = income_data
                DataManager.save_data_to_json(income_data, 'income_budget.json')
                st.success("项目信息已更新并保存！")
    
            # 显示筛选后统计
            total_revenue_filtered = filtered_df['预期收入'].sum()
            total_adjusted_revenue_filtered = filtered_df['纠偏后收入'].sum()
            total_contract_filtered = filtered_df['合同金额'].sum()
            col1, col2 = st.columns(2)
            with col1:
                st.metric("💰 筛选后总预期收入", f"{total_revenue_filtered:.2f} 万元", f"合同总额: {total_contract_filtered:.2f} 万元")
            with col2:
                delta_pct = ((total_adjusted_revenue_filtered - total_revenue_filtered) / total_revenue_filtered * 100) if total_revenue_filtered > 0 else 0.0
                st.metric("💰 筛选后总纠偏后收入", f"{total_adjusted_revenue_filtered:.2f} 万元", f"调整幅度: {delta_pct:+.1f}%")
            st.info(f"共显示 {len(filtered_df)} 个项目 (总计 {len(full_data)} 个)")
    
            # 全局汇总指标（用于下方图表）
            total_revenue_all = full_data['预期收入'].sum()
            total_adjusted_revenue_all = full_data['纠偏后收入'].sum()
            total_contract_all = full_data['合同金额'].sum()
    
        # ==================== 全局汇总 & 可视化 ====================
        st.divider()
        st.subheader("📊 全局预测概览")
    
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("项目总数", len(full_data))
        with col2:
            st.metric("总合同金额", f"{total_contract_all:.2f} 万元")
        with col3:
            st.metric("总纠偏后收入", f"{total_adjusted_revenue_all:.2f} 万元")
    
        if not full_data.empty:
            summary_df = st.session_state.data_manager['income'].generate_summary()
    
            st.header("📈 预测可视化")
            tab1, tab2, tab3 = st.tabs(["季度分布", "业务线分析", "时间衰减趋势"])
    
            with tab1:
                quarterly_data = summary_df[summary_df['类别'] == '季度收入']
                if not quarterly_data.empty:
                    quarterly_data = quarterly_data.copy()
                    quarterly_data['项目_中文'] = quarterly_data['项目'].apply(lambda x: x.replace('-Q', '年Q'))
                    fig_q = go.Figure()
                    fig_q.add_trace(go.Bar(x=quarterly_data['项目_中文'], y=quarterly_data['金额'], name='纠偏后收入', marker_color='#1a2a6c'))
                    fig_q.add_trace(go.Scatter(x=quarterly_data['项目_中文'], y=quarterly_data['累计占比'], name='累计占比', yaxis='y2', mode='lines+markers', line=dict(color='#ff2e2e', width=3), marker=dict(size=8)))
                    fig_q.update_layout(
                        title='季度收入分布与累计占比',
                        xaxis_title='季度',
                        yaxis_title='纠偏后收入 (万元)',
                        yaxis2=dict(title='累计占比 (%)', overlaying='y', side='right'),
                        hovermode='x unified',
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)'
                    )
                    st.plotly_chart(fig_q, use_container_width=True)
                    quarterly_display = quarterly_data[['类别', '项目_中文', '金额', '项目数', '平均衰减', '累计占比', '合同总额']].rename(columns={'项目_中文': '项目'})
                    st.dataframe(quarterly_display.style.format({
                        '金额': '{:.2f}', '累计占比': '{:.1f}%', '项目数': '{:.0f}', '平均衰减': '{:.4f}', '合同总额': '{:.2f}'
                    }), use_container_width=True)
                else:
                    st.info("暂无季度数据可显示")
    
            with tab2:
                business_data = summary_df[summary_df['类别'] == '业务线']
                if not business_data.empty:
                    fig_b = px.pie(business_data, values='金额', names='项目', title='业务线收入贡献', hole=0.3, color_discrete_sequence=px.colors.qualitative.Set3)
                    fig_b.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_b, use_container_width=True)
    
                    fig_b2 = go.Figure()
                    fig_b2.add_trace(go.Bar(x=business_data['项目'], y=business_data['金额'], name='纠偏后收入', marker_color='#1a2a6c'))
                    fig_b2.add_trace(go.Bar(x=business_data['项目'], y=business_data['合同总额'], name='合同总额', marker_color='#83c9ff'))
                    fig_b2.update_layout(
                        barmode='group',
                        title='业务线收入对比',
                        xaxis_title='业务线',
                        yaxis_title='金额 (万元)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)'
                    )
                    st.plotly_chart(fig_b2, use_container_width=True)
    
                    st.dataframe(business_data.style.format({
                        '合同总额': '{:.2f}', '金额': '{:.2f}', '贡献率': '{:.1f}%', '项目数': '{:.0f}'
                    }), use_container_width=True)
                else:
                    st.info("暂无业务线数据可显示")
    
            with tab3:
                decay_data = full_data.copy()
                decay_data['交付年月'] = pd.to_datetime(decay_data['交付日期']).dt.strftime('%Y-%m')
                fig_adj = px.scatter(
                    decay_data,
                    x='预期收入',
                    y='纠偏后收入',
                    size='纠偏后收入',
                    color='业务线',
                    hover_name='项目名称',
                    hover_data=['合同金额', '保守成单率', '时间衰减因子'],
                    title='纠偏后收入 vs 预期收入'
                )
                max_val = max(decay_data['预期收入'].max(), decay_data['纠偏后收入'].max())
                fig_adj.add_trace(go.Scatter(x=[0, max_val], y=[0, max_val], mode='lines', name='y=x参考线', line=dict(color='red', dash='dash')))
                fig_adj.update_layout(
                    xaxis_title='预期收入',
                    yaxis_title='纠偏后收入',
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig_adj, use_container_width=True)
    
                months = np.arange(0, 25)
                decay_values = np.exp(-0.0315 * months)
                fig_curve = go.Figure()
                fig_curve.add_trace(go.Scatter(x=months, y=decay_values, mode='lines+markers', name='λ=0.0315', line=dict(color='#1a2a6c', width=3)))
                fig_curve.update_layout(
                    title='时间衰减曲线',
                    xaxis_title='月份数',
                    yaxis_title='衰减因子',
                    yaxis_range=[0, 1.05],
                    hovermode='x unified',
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig_curve, use_container_width=True)

    elif st.session_state.selected_page == "成本管理":
        st.header("📦 成本管理")
        tab1, tab2, tab3, tab4 = st.tabs(["物料支出分析", "人工成本分析", "行政费用管理", "偶然收支管理"])
        
        with tab1:
            st.subheader("物料支出分析")
            if not st.session_state.data_manager['income'].data.empty:
                material_cost_df = st.session_state.data_manager['income'].generate_material_cost_data()
                if not material_cost_df.empty:
                    total_material_cost = material_cost_df['物料成本'].sum()
                    total_revenue = st.session_state.data_manager['income'].data['纠偏后收入'].sum()
                    col1, col2, col3 = st.columns(3)
                    with col1: st.metric("总物料成本", f"{total_material_cost:.2f} 万元")
                    with col2: st.metric("毛利率", f"{((total_revenue - total_material_cost) / total_revenue * 100):.1f}%" if total_revenue > 0 else "0.0%")
                    with col3: st.metric("物料成本占比", f"{(total_material_cost / total_revenue * 100):.1f}%" if total_revenue > 0 else "0.0%")
                    business_material_summary = material_cost_df.groupby('业务线').agg({'物料成本': 'sum', '物料支出比例': 'mean'}).reset_index()
                    st.subheader("业务线物料支出分布")
                    fig_material = px.pie(business_material_summary, values='物料成本', names='业务线', title='业务线物料支出分布', hole=0.3, color_discrete_sequence=px.colors.qualitative.Set3)
                    fig_material.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_material, use_container_width=True)
                    st.subheader("物料支出时间分布")
                    monthly_material_cost = material_cost_df.groupby('支出月份')['物料成本'].sum().reset_index()
                    monthly_material_cost['支出月份'] = pd.to_datetime(monthly_material_cost['支出月份'])
                    monthly_material_cost = monthly_material_cost.sort_values('支出月份')
                    monthly_material_cost['支出月份_中文'] = monthly_material_cost['支出月份'].apply(lambda x: f"{x.year}年{x.month}月")
                    fig_monthly_material = px.line(monthly_material_cost, x='支出月份_中文', y='物料成本', title='月度物料支出趋势', markers=True)
                    fig_monthly_material.update_layout(xaxis_title='月份', yaxis_title='物料成本 (万元)', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig_monthly_material, use_container_width=True)
                    st.subheader("物料支出详情")
                    material_display = material_cost_df[['项目名称', '业务线', '支出月份', '物料成本', '物料支出比例']].copy()
                    material_display['支出月份_中文'] = pd.to_datetime(material_display['支出月份']).apply(lambda x: f"{x.year}年{x.month}月")
                    material_display = material_display.rename(columns={'支出月份': '支出月份_英文'})
                    material_display = material_display.rename(columns={'支出月份_中文': '支出月份'})
                    st.dataframe(material_display.style.format({'物料成本': '{:.2f}', '物料支出比例': '{:.1f}%'}), use_container_width=True)
                else: st.info("暂无物料支出数据，需要先添加收入预算项目。")
            else: st.info("暂无项目数据。请先添加收入预算项目以进行物料支出分析。")
        
        with tab2:
            st.subheader("人工成本分析")
            with st.expander("➕ 添加人工成本"):
                col1, col2 = st.columns(2)
                with col1:
                    cost_type = st.selectbox("成本类型", ["销售费用", "制造费用", "研发费用", "管理费用", "其他"], key="labor_cost_type")
                    person_dept = st.text_input("人员/部门", placeholder="如：销售团队、生产部门等", key="person_dept")
                    monthly_cost = st.number_input("月度成本 (万元)", min_value=0.0, value=5.0, step=0.1, key="labor_monthly_cost")
                with col2:
                    start_date = st.date_input("开始日期", value=date.today().replace(day=1), key="labor_start_date")
                    end_date = st.date_input("结束日期", value=date.today().replace(day=1) + pd.DateOffset(months=12) - pd.DateOffset(days=1), key="labor_end_date")
                if st.button("添加人工成本项目", type="secondary", key="add_labor"):
                    if not person_dept: 
                        st.error("人员/部门不能为空")
                    else:
                        new_labor = {'成本类型': cost_type, '人员/部门': person_dept, '月度成本': round(monthly_cost, 2), '开始日期': start_date, '结束日期': end_date}
                        new_df = pd.DataFrame([new_labor])
                        if st.session_state.data_manager['labor'].data.empty:
                            st.session_state.data_manager['labor'].data = new_df.copy()
                        else:
                            st.session_state.data_manager['labor'].data = pd.concat([st.session_state.data_manager['labor'].data, new_df], ignore_index=True)
                        DataManager.save_data_to_json(st.session_state.data_manager['labor'].data, 'labor_budget.json')
                        st.success(f"人工成本项目 '{person_dept}' 已成功添加！")
            
            st.subheader("📥 人工成本模板导入")
            labor_template_df = generate_template_data()['labor']
            labor_template_csv = labor_template_df.to_csv(index=False).encode('utf-8')
            st.download_button(label="下载人工成本导入模板", data=labor_template_csv, file_name="人工成本导入模板.csv", mime="text/csv")
            labor_uploaded_file = st.file_uploader("上传人工成本数据 (CSV/Excel)", type=['csv', 'xlsx', 'xls'], key="labor_upload")
            if labor_uploaded_file is not None:
                if st.button("导入人工成本数据", type="primary", key="import_labor"):
                    try:
                        if labor_uploaded_file.name.endswith('.csv'): 
                            df = pd.read_csv(labor_uploaded_file)
                        elif labor_uploaded_file.name.endswith(('.xlsx', '.xls')): 
                            df = pd.read_excel(labor_uploaded_file)
                        required_columns = ['成本类型', '人员/部门', '月度成本', '开始日期', '结束日期']
                        missing_columns = [col for col in required_columns if col not in df.columns]
                        if missing_columns: 
                            st.error(f"文件缺少必要列: {', '.join(missing_columns)}")
                        else:
                            df['开始日期'] = pd.to_datetime(df['开始日期'])
                            df['结束日期'] = pd.to_datetime(df['结束日期'])
                            df['月度成本'] = df['月度成本'].round(2)
                            if st.session_state.data_manager['labor'].data.empty:
                                st.session_state.data_manager['labor'].data = df.copy()
                            else:
                                st.session_state.data_manager['labor'].data = pd.concat([st.session_state.data_manager['labor'].data, df], ignore_index=True)
                            DataManager.save_data_to_json(st.session_state.data_manager['labor'].data, 'labor_budget.json')
                            st.success(f"成功导入 {len(df)} 个人工成本项目！")
                    except Exception as e: 
                        st.error(f"导入人工成本数据时出错: {str(e)}")
            
            if not st.session_state.data_manager['labor'].data.empty:
                st.subheader("人工成本明细")
                # 确保日期列是datetime类型
                labor_df = st.session_state.data_manager['labor'].data.copy()
                for col in ['开始日期', '结束日期']:
                    if col in labor_df.columns:
                        labor_df[col] = pd.to_datetime(labor_df[col], errors='coerce')
                
                # 添加删除功能
                labor_df['删除'] = False  # 添加删除列
                edited_labor_df = st.data_editor(
                    labor_df.style.format({'月度成本': '{:.2f}'}),
                    use_container_width=True, 
                    num_rows="dynamic",
                    key="labor_data_editor",
                    column_config={
                        "月度成本": st.column_config.NumberColumn("月度成本", help="每月的人工成本", min_value=0.0, step=0.01, default=0.0),
                        "删除": st.column_config.CheckboxColumn("删除", default=False)
                    }
                )
                
                # 处理删除操作
                if '删除' in edited_labor_df.columns:
                    rows_to_delete = edited_labor_df[edited_labor_df['删除'] == True]
                    if not rows_to_delete.empty:
                        if st.button(f"🗑️ 删除 {len(rows_to_delete)} 项选中的人工成本", type="secondary"):
                            st.session_state.data_manager['labor'].data = edited_labor_df[edited_labor_df['删除'] == False].drop(columns=['删除']).copy()
                            DataManager.save_data_to_json(st.session_state.data_manager['labor'].data, 'labor_budget.json')
                            st.success(f"已删除 {len(rows_to_delete)} 项人工成本！")
                            st.rerun()  # 刷新页面以更新显示
                
                # 处理编辑操作（排除删除列）
                edited_labor_df_filtered = edited_labor_df.drop(columns=['删除']) if '删除' in edited_labor_df.columns else edited_labor_df
                if not edited_labor_df_filtered.equals(st.session_state.data_manager['labor'].data):
                    # 确保日期列的类型正确
                    for col in ['开始日期', '结束日期']:
                        if col in edited_labor_df_filtered.columns:
                            edited_labor_df_filtered[col] = pd.to_datetime(edited_labor_df_filtered[col], errors='coerce')
                    
                    # 确保数值列的类型正确
                    edited_labor_df_filtered['月度成本'] = edited_labor_df_filtered['月度成本'].round(2)
                    st.session_state.data_manager['labor'].data = edited_labor_df_filtered.copy()
                    DataManager.save_data_to_json(st.session_state.data_manager['labor'].data, 'labor_budget.json')
                    st.success("人工成本数据已更新并保存！")
                
                # 显示月度成本数据
                labor_monthly_df = st.session_state.data_manager['labor'].generate_cost_data()
                if not labor_monthly_df.empty:
                    total_labor_cost = labor_monthly_df['成本金额'].sum()
                    monthly_labor_avg = labor_monthly_df.groupby('支出月份')['成本金额'].sum().mean()
                    col1, col2 = st.columns(2)
                    with col1: 
                        st.metric("总人工成本", f"{total_labor_cost:.2f} 万元")
                    with col2: 
                        st.metric("月均人工成本", f"{monthly_labor_avg:.2f} 万元")
                    
                    st.subheader("成本类型分布")
                    type_summary = labor_monthly_df.groupby('成本类型')['成本金额'].sum().reset_index()
                    fig_labor_type = px.pie(type_summary, values='成本金额', names='成本类型', title='人工成本类型分布', hole=0.3, color_discrete_sequence=px.colors.qualitative.Set3)
                    fig_labor_type.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_labor_type, use_container_width=True)
                    
                    st.subheader("月度人工成本趋势")
                    monthly_summary = labor_monthly_df.groupby('支出月份')['成本金额'].sum().reset_index()
                    monthly_summary['支出月份'] = pd.to_datetime(monthly_summary['支出月份'])
                    monthly_summary = monthly_summary.sort_values('支出月份')
                    monthly_summary['支出月份_中文'] = monthly_summary['支出月份'].apply(lambda x: f"{x.year}年{x.month}月")
                    monthly_summary = monthly_summary.rename(columns={'支出月份': '支出月份_英文'})
                    monthly_summary = monthly_summary.rename(columns={'支出月份_中文': '支出月份'})
                    fig_labor_monthly = px.line(monthly_summary, x='支出月份', y='成本金额', title='月度人工成本趋势', markers=True)
                    fig_labor_monthly.update_layout(xaxis_title='月份', yaxis_title='人工成本 (万元)', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig_labor_monthly, use_container_width=True)
                    
                    st.subheader("人工成本详情")
                    labor_display = labor_monthly_df.copy()
                    labor_display['支出月份_中文'] = pd.to_datetime(labor_display['支出月份']).apply(lambda x: f"{x.year}年{x.month}月")
                    labor_display = labor_display.rename(columns={'支出月份': '支出月份_英文'})
                    labor_display = labor_display.rename(columns={'支出月份_中文': '支出月份'})
                    st.dataframe(labor_display.style.format({'成本金额': '{:.2f}'}), use_container_width=True)
            else: 
                st.info("暂无人工成本数据。请添加人工成本项目或导入数据。")

        # 在行政费用管理部分，替换原有的数据编辑部分
        with tab3:
            st.subheader("行政费用管理")
            with st.expander("➕ 添加行政费用"):
                col1, col2 = st.columns(2)
                with col1:
                    # 添加新的费用类型选项
                    expense_type = st.selectbox("费用类型", ["房租费用", "水电费用", "办公用品", "差旅费用", "研发支出", "营销支出", "售前支出", "财务税费", "其他"], key="admin_expense_type")
                    expense_item = st.text_input("费用项目", placeholder="如：办公室租金、水电费等", key="expense_item")
                    monthly_cost = st.number_input("月度成本 (万元)", min_value=0.0, value=1.0, step=0.1, key="admin_monthly_cost")
                with col2:
                    start_date = st.date_input("开始日期", value=date.today().replace(day=1), key="admin_start_date")
                    end_date = st.date_input("结束日期", value=date.today().replace(day=1) + pd.DateOffset(months=12) - pd.DateOffset(days=1), key="admin_end_date")
                    payment_frequency = st.selectbox("付款频率", ["月度", "季度", "年度"], key="payment_frequency")
                if st.button("添加行政费用项目", type="secondary", key="add_admin"):
                    if not expense_item: 
                        st.error("费用项目不能为空")
                    else:
                        new_admin = {'费用类型': expense_type, '费用项目': expense_item, '月度成本': round(monthly_cost, 2), '开始日期': start_date, '结束日期': end_date, '付款频率': payment_frequency}
                        new_df = pd.DataFrame([new_admin])
                        if st.session_state.data_manager['admin'].data.empty:
                            st.session_state.data_manager['admin'].data = new_df.copy()
                        else:
                            st.session_state.data_manager['admin'].data = pd.concat([st.session_state.data_manager['admin'].data, new_df], ignore_index=True)
                        DataManager.save_data_to_json(st.session_state.data_manager['admin'].data, 'admin_budget.json')
                        st.success(f"行政费用项目 '{expense_item}' 已成功添加！")
            
            st.subheader("📥 行政费用模板导入")
            admin_template_df = generate_template_data()['admin']
            admin_template_csv = admin_template_df.to_csv(index=False).encode('utf-8')
            st.download_button(label="下载行政费用导入模板", data=admin_template_csv, file_name="行政费用导入模板.csv", mime="text/csv")
            admin_uploaded_file = st.file_uploader("上传行政费用数据 (CSV/Excel)", type=['csv', 'xlsx', 'xls'], key="admin_upload")
            if admin_uploaded_file is not None:
                if st.button("导入行政费用数据", type="primary", key="import_admin"):
                    try:
                        if admin_uploaded_file.name.endswith('.csv'): 
                            df = pd.read_csv(admin_uploaded_file)
                        elif admin_uploaded_file.name.endswith(('.xlsx', '.xls')): 
                            df = pd.read_excel(admin_uploaded_file)
                        required_columns = ['费用类型', '费用项目', '月度成本', '开始日期', '结束日期', '付款频率']
                        missing_columns = [col for col in required_columns if col not in df.columns]
                        if missing_columns: 
                            st.error(f"文件缺少必要列: {', '.join(missing_columns)}")
                        else:
                            df['开始日期'] = pd.to_datetime(df['开始日期'])
                            df['结束日期'] = pd.to_datetime(df['结束日期'])
                            df['月度成本'] = df['月度成本'].round(2)
                            if st.session_state.data_manager['admin'].data.empty:
                                st.session_state.data_manager['admin'].data = df.copy()
                            else:
                                st.session_state.data_manager['admin'].data = pd.concat([st.session_state.data_manager['admin'].data, df], ignore_index=True)
                            DataManager.save_data_to_json(st.session_state.data_manager['admin'].data, 'admin_budget.json')
                            st.success(f"成功导入 {len(df)} 个行政费用项目！")
                    except Exception as e: 
                        st.error(f"导入行政费用数据时出错: {str(e)}")
            
            if not st.session_state.data_manager['admin'].data.empty:
                st.subheader("行政费用明细")
                # 确保日期列是datetime类型
                admin_df = st.session_state.data_manager['admin'].data.copy()
                for col in ['开始日期', '结束日期']:
                    if col in admin_df.columns:
                        admin_df[col] = pd.to_datetime(admin_df[col], errors='coerce')
                
                # 添加删除功能
                admin_df['删除'] = False  # 添加删除列
                edited_admin_df = st.data_editor(
                    admin_df.style.format({'月度成本': '{:.2f}'}),
                    use_container_width=True, 
                    num_rows="dynamic",
                    key="admin_data_editor",
                    column_config={
                        "月度成本": st.column_config.NumberColumn("月度成本", help="每月的行政费用", min_value=0.0, step=0.01, default=0.0),
                        "删除": st.column_config.CheckboxColumn("删除", default=False)
                    }
                )
                
                # 处理删除操作
                if '删除' in edited_admin_df.columns:
                    rows_to_delete = edited_admin_df[edited_admin_df['删除'] == True]
                    if not rows_to_delete.empty:
                        if st.button(f"🗑️ 删除 {len(rows_to_delete)} 项选中的行政费用", type="secondary"):
                            st.session_state.data_manager['admin'].data = edited_admin_df[edited_admin_df['删除'] == False].drop(columns=['删除']).copy()
                            DataManager.save_data_to_json(st.session_state.data_manager['admin'].data, 'admin_budget.json')
                            st.success(f"已删除 {len(rows_to_delete)} 项行政费用！")
                            st.rerun()  # 刷新页面以更新显示
                
                # 处理编辑操作（排除删除列）
                edited_admin_df_filtered = edited_admin_df.drop(columns=['删除']) if '删除' in edited_admin_df.columns else edited_admin_df
                if not edited_admin_df_filtered.equals(st.session_state.data_manager['admin'].data):
                    # 确保日期列的类型正确
                    for col in ['开始日期', '结束日期']:
                        if col in edited_admin_df_filtered.columns:
                            edited_admin_df_filtered[col] = pd.to_datetime(edited_admin_df_filtered[col], errors='coerce')
                    
                    # 确保数值列的类型正确并保留两位小数
                    edited_admin_df_filtered['月度成本'] = edited_admin_df_filtered['月度成本'].round(2)
                    st.session_state.data_manager['admin'].data = edited_admin_df_filtered.copy()
                    DataManager.save_data_to_json(st.session_state.data_manager['admin'].data, 'admin_budget.json')
                    st.success("行政费用数据已更新并保存！")
                
                # 显示月度费用数据
                admin_monthly_df = st.session_state.data_manager['admin'].generate_cost_data()
                if not admin_monthly_df.empty:
                    total_admin_cost = admin_monthly_df['月度成本'].sum()
                    monthly_admin_avg = admin_monthly_df.groupby('支出月份')['月度成本'].sum().mean()
                    col1, col2 = st.columns(2)
                    with col1: 
                        st.metric("总行政费用", f"{total_admin_cost:.2f} 万元")
                    with col2: 
                        st.metric("月均行政费用", f"{monthly_admin_avg:.2f} 万元")
                    
                    st.subheader("费用类型分布")
                    type_summary = admin_monthly_df.groupby('费用类型')['月度成本'].sum().reset_index()
                    fig_admin_type = px.pie(type_summary, values='月度成本', names='费用类型', title='行政费用类型分布', hole=0.3, color_discrete_sequence=px.colors.qualitative.Set3)
                    fig_admin_type.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_admin_type, use_container_width=True)
                    
                    st.subheader("月度行政费用趋势")
                    monthly_summary = admin_monthly_df.groupby('支出月份')['月度成本'].sum().reset_index()
                    monthly_summary['支出月份'] = pd.to_datetime(monthly_summary['支出月份'])
                    monthly_summary = monthly_summary.sort_values('支出月份')
                    monthly_summary['支出月份_中文'] = monthly_summary['支出月份'].apply(lambda x: f"{x.year}年{x.month}月")
                    monthly_summary = monthly_summary.rename(columns={'支出月份': '支出月份_英文'})
                    monthly_summary = monthly_summary.rename(columns={'支出月份_中文': '支出月份'})
                    fig_admin_monthly = px.line(monthly_summary, x='支出月份', y='月度成本', title='月度行政费用趋势', markers=True)
                    fig_admin_monthly.update_layout(xaxis_title='月份', yaxis_title='行政费用 (万元)', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig_admin_monthly, use_container_width=True)
                    
                    st.subheader("行政费用详情")
                    admin_display = admin_monthly_df.copy()
                    admin_display['支出月份_中文'] = pd.to_datetime(admin_display['支出月份']).apply(lambda x: f"{x.year}年{x.month}月")
                    admin_display = admin_display.rename(columns={'支出月份': '支出月份_英文'})
                    admin_display = admin_display.rename(columns={'支出月份_中文': '支出月份'})
                    st.dataframe(admin_display.style.format({'月度成本': '{:.2f}'}), use_container_width=True)
            else: 
                st.info("暂无行政费用数据。请添加行政费用项目或导入数据。")
        
        with tab4:
            st.subheader("偶然收入管理")
            with st.expander("➕ 添加偶然收入"):
                col1, col2 = st.columns(2)
                with col1:
                    income_name = st.text_input("收入名称", placeholder="如：政府补贴、投资收益等")
                    income_amount = st.number_input("收入金额 (万元)", min_value=0.0, step=0.01)
                with col2:
                    income_date = st.date_input("收入日期", value=date.today())
                    income_type = st.text_input("收入类型", placeholder="如：补贴、投资、其他")
                if st.button("添加偶然收入"):
                    if not income_name: 
                        st.error("收入名称不能为空")
                    else:
                        new_income = {'收入名称': income_name, '收入金额': round(income_amount, 2), '收入日期': income_date, '收入类型': income_type}
                        new_df = pd.DataFrame([new_income])
                        if st.session_state.data_manager['occasional']['occasional_income'].empty:
                            st.session_state.data_manager['occasional']['occasional_income'] = new_df
                        else:
                            st.session_state.data_manager['occasional']['occasional_income'] = pd.concat([st.session_state.data_manager['occasional']['occasional_income'], new_df], ignore_index=True)
                        DataManager.save_data_to_json(st.session_state.data_manager['occasional']['occasional_income'], 'occasional_income.json')
                        st.success(f"偶然收入 '{income_name}' 已添加！")
            
            if not st.session_state.data_manager['occasional']['occasional_income'].empty:
                # 确保日期列是datetime类型
                occasional_income_df = st.session_state.data_manager['occasional']['occasional_income'].copy()
                if '收入日期' in occasional_income_df.columns:
                    occasional_income_df['收入日期'] = pd.to_datetime(occasional_income_df['收入日期'], errors='coerce')
                
                # 添加删除功能
                occasional_income_df['删除'] = False  # 添加删除列
                edited_income = st.data_editor(
                    occasional_income_df.style.format({'收入金额': '{:.2f}'}),
                    use_container_width=True,
                    key="occasional_income_editor",
                    column_config={
                        "收入金额": st.column_config.NumberColumn("收入金额", help="收入金额", min_value=0.0, step=0.01, default=0.0),
                        "删除": st.column_config.CheckboxColumn("删除", default=False)
                    }
                )
                
                # 处理删除操作
                if '删除' in edited_income.columns:
                    rows_to_delete = edited_income[edited_income['删除'] == True]
                    if not rows_to_delete.empty:
                        if st.button(f"🗑️ 删除 {len(rows_to_delete)} 项选中的偶然收入", type="secondary"):
                            st.session_state.data_manager['occasional']['occasional_income'] = edited_income[edited_income['删除'] == False].drop(columns=['删除']).copy()
                            DataManager.save_data_to_json(st.session_state.data_manager['occasional']['occasional_income'], 'occasional_income.json')
                            st.success(f"已删除 {len(rows_to_delete)} 项偶然收入！")
                            st.rerun()  # 刷新页面以更新显示
                
                # 处理编辑操作（排除删除列）
                edited_income_filtered = edited_income.drop(columns=['删除']) if '删除' in edited_income.columns else edited_income
                if not edited_income_filtered.equals(st.session_state.data_manager['occasional']['occasional_income']):
                    # 确保日期列的类型正确
                    if '收入日期' in edited_income_filtered.columns:
                        edited_income_filtered['收入日期'] = pd.to_datetime(edited_income_filtered['收入日期'], errors='coerce')
                    
                    # 确保数值列的类型正确并保留两位小数
                    edited_income_filtered['收入金额'] = edited_income_filtered['收入金额'].round(2)
                    st.session_state.data_manager['occasional']['occasional_income'] = edited_income_filtered
                    DataManager.save_data_to_json(st.session_state.data_manager['occasional']['occasional_income'], 'occasional_income.json')
                    st.success("偶然收入数据已更新！")
                
                total_occasional_income = st.session_state.data_manager['occasional']['occasional_income']['收入金额'].sum()
                st.metric("总偶然收入", f"{total_occasional_income:.2f} 万元")
            
            st.subheader("偶然支出管理")
            with st.expander("➕ 添加偶然支出"):
                col1, col2 = st.columns(2)
                with col1:
                    expense_name = st.text_input("支出名称", placeholder="如：罚款、维修、捐赠等")
                    expense_amount = st.number_input("支出金额 (万元)", min_value=0.0, step=0.01)
                with col2:
                    expense_date = st.date_input("支出日期", value=date.today())
                    expense_type = st.text_input("支出类型", placeholder="如：罚款、维修、捐赠")
                if st.button("添加偶然支出"):
                    if not expense_name: 
                        st.error("支出名称不能为空")
                    else:
                        new_expense = {'支出名称': expense_name, '支出金额': round(expense_amount, 2), '支出日期': expense_date, '支出类型': expense_type}
                        new_df = pd.DataFrame([new_expense])
                        if st.session_state.data_manager['occasional']['occasional_expense'].empty:
                            st.session_state.data_manager['occasional']['occasional_expense'] = new_df
                        else:
                            st.session_state.data_manager['occasional']['occasional_expense'] = pd.concat([st.session_state.data_manager['occasional']['occasional_expense'], new_df], ignore_index=True)
                        DataManager.save_data_to_json(st.session_state.data_manager['occasional']['occasional_expense'], 'occasional_expense.json')
                        st.success(f"偶然支出 '{expense_name}' 已添加！")
            
            if not st.session_state.data_manager['occasional']['occasional_expense'].empty:
                # 确保日期列是datetime类型
                occasional_expense_df = st.session_state.data_manager['occasional']['occasional_expense'].copy()
                if '支出日期' in occasional_expense_df.columns:
                    occasional_expense_df['支出日期'] = pd.to_datetime(occasional_expense_df['支出日期'], errors='coerce')
                
                # 添加删除功能
                occasional_expense_df['删除'] = False  # 添加删除列
                edited_expense = st.data_editor(
                    occasional_expense_df.style.format({'支出金额': '{:.2f}'}),
                    use_container_width=True,
                    key="occasional_expense_editor",
                    column_config={
                        "支出金额": st.column_config.NumberColumn("支出金额", help="支出金额", min_value=0.0, step=0.01, default=0.0),
                        "删除": st.column_config.CheckboxColumn("删除", default=False)
                    }
                )
                
                # 处理删除操作
                if '删除' in edited_expense.columns:
                    rows_to_delete = edited_expense[edited_expense['删除'] == True]
                    if not rows_to_delete.empty:
                        if st.button(f"🗑️ 删除 {len(rows_to_delete)} 项选中的偶然支出", type="secondary"):
                            st.session_state.data_manager['occasional']['occasional_expense'] = edited_expense[edited_expense['删除'] == False].drop(columns=['删除']).copy()
                            DataManager.save_data_to_json(st.session_state.data_manager['occasional']['occasional_expense'], 'occasional_expense.json')
                            st.success(f"已删除 {len(rows_to_delete)} 项偶然支出！")
                            st.rerun()  # 刷新页面以更新显示
                
                # 处理编辑操作（排除删除列）
                edited_expense_filtered = edited_expense.drop(columns=['删除']) if '删除' in edited_expense.columns else edited_expense
                if not edited_expense_filtered.equals(st.session_state.data_manager['occasional']['occasional_expense']):
                    # 确保日期列的类型正确
                    if '支出日期' in edited_expense_filtered.columns:
                        edited_expense_filtered['支出日期'] = pd.to_datetime(edited_expense_filtered['支出日期'], errors='coerce')
                    
                    # 确保数值列的类型正确并保留两位小数
                    edited_expense_filtered['支出金额'] = edited_expense_filtered['支出金额'].round(2)
                    st.session_state.data_manager['occasional']['occasional_expense'] = edited_expense_filtered
                    DataManager.save_data_to_json(st.session_state.data_manager['occasional']['occasional_expense'], 'occasional_expense.json')
                    st.success("偶然支出数据已更新！")

                
                total_occasional_expense = st.session_state.data_manager['occasional']['occasional_expense']['支出金额'].sum()
                st.metric("总偶然支出", f"{total_occasional_expense:.2f} 万元")

    elif st.session_state.selected_page == "现金流分析":
        st.header("💸 现金流分析")
        if not st.session_state.data_manager['income'].data.empty:
            cash_flow_df = st.session_state.data_manager['income'].generate_cash_flow_data()
            if not cash_flow_df.empty:
                monthly_cash_flow = cash_flow_df.groupby('支付月份').agg({'金额': 'sum'}).reset_index()
                monthly_cash_flow['支付月份'] = pd.to_datetime(monthly_cash_flow['支付月份'])
                monthly_cash_flow = monthly_cash_flow.sort_values('支付月份')
                monthly_cash_flow['支付月份_中文'] = monthly_cash_flow['支付月份'].apply(lambda x: f"{x.year}年{x.month}月")
                monthly_cash_flow = monthly_cash_flow.rename(columns={'支付月份': '支付月份_英文'})
                monthly_cash_flow = monthly_cash_flow.rename(columns={'支付月份_中文': '支付月份'})
                fig_cf = go.Figure()
                for cash_type in cash_flow_df['现金流类型'].unique():
                    type_data = cash_flow_df[cash_flow_df['现金流类型'] == cash_type]
                    monthly_type = type_data.groupby('支付月份').agg({'金额': 'sum'}).reset_index()
                    monthly_type['支付月份'] = pd.to_datetime(monthly_type['支付月份'])
                    monthly_type = monthly_type.sort_values('支付月份')
                    monthly_type['支付月份_中文'] = monthly_type['支付月份'].apply(lambda x: f"{x.year}年{x.month}月")
                    monthly_type = monthly_type.rename(columns={'支付月份': '支付月份_英文'})
                    monthly_type = monthly_type.rename(columns={'支付月份_中文': '支付月份'})
                    fig_cf.add_trace(go.Bar(x=monthly_type['支付月份'], y=monthly_type['金额'], name=cash_type, text=monthly_type['金额'], textposition='auto'))
                fig_cf.update_layout(title='月度现金流分布', xaxis_title='月份', yaxis_title='金额 (万元)', barmode='stack', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_cf, use_container_width=True)
                st.subheader("现金流汇总")
                cash_flow_summary = cash_flow_df.groupby('现金流类型').agg({'金额': 'sum'}).reset_index()
                cash_flow_summary['占比'] = cash_flow_summary['金额'] / cash_flow_summary['金额'].sum() * 100
                st.dataframe(cash_flow_summary.style.format({'金额': '{:.2f}', '占比': '{:.1f}%'}), use_container_width=True)
                st.subheader("现金流详情")
                cash_flow_display = cash_flow_df[['项目名称', '现金流类型', '支付月份', '金额', '付款比例', '业务线']].copy()
                cash_flow_display['支付月份_中文'] = pd.to_datetime(cash_flow_display['支付月份']).apply(lambda x: f"{x.year}年{x.month}月")
                cash_flow_display = cash_flow_display.rename(columns={'支付月份': '支付月份_英文'})
                cash_flow_display = cash_flow_display.rename(columns={'支付月份_中文': '支付月份'})
                st.dataframe(cash_flow_display.style.format({'金额': '{:.2f}'}), use_container_width=True)
                st.subheader("收入与现金流对比")
                col1, col2 = st.columns(2)
                with col1:
                    total_adjusted_revenue = st.session_state.data_manager['income'].data['纠偏后收入'].sum()
                    st.metric("总收入", f"{total_adjusted_revenue:.2f} 万元")
                with col2:
                    total_cash_flow = cash_flow_df['金额'].sum()
                    st.metric("总现金流", f"{total_cash_flow:.2f} 万元")
                cash_flow_by_month = cash_flow_df.groupby('支付月份')['金额'].sum().reset_index()
                cash_flow_by_month['支付月份'] = pd.to_datetime(cash_flow_by_month['支付月份'])
                cash_flow_by_month = cash_flow_by_month.sort_values('支付月份')
                cash_flow_by_month['支付月份_中文'] = cash_flow_by_month['支付月份'].apply(lambda x: f"{x.year}年{x.month}月")
                cash_flow_by_month = cash_flow_by_month.rename(columns={'支付月份': '支付月份_英文'})
                cash_flow_by_month = cash_flow_by_month.rename(columns={'支付月份_中文': '支付月份'})
                fig_monthly = px.line(cash_flow_by_month, x='支付月份', y='金额', title='月度现金流趋势', markers=True)
                fig_monthly.update_layout(xaxis_title='月份', yaxis_title='金额 (万元)', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_monthly, use_container_width=True)
                st.subheader("💰 Runway分析")
                if st.session_state.current_cash_balance > 0:
                    monthly_income = cash_flow_df.groupby('支付月份')['金额'].sum().reset_index()
                    monthly_income['支付月份'] = pd.to_datetime(monthly_income['支付月份'])
                    material_df = st.session_state.data_manager['income'].generate_material_cost_data()
                    labor_df = st.session_state.data_manager['labor'].generate_cost_data()
                    admin_df = st.session_state.data_manager['admin'].generate_cost_data()
                    all_months = set()
                    if not monthly_income.empty: all_months.update(monthly_income['支付月份'].dt.to_period('M').astype(str))
                    if not material_df.empty: all_months.update(material_df['支出月份'])
                    if not labor_df.empty: all_months.update(labor_df['支出月份'])
                    if not admin_df.empty: all_months.update(admin_df['支出月份'])
                    if not st.session_state.data_manager['occasional']['occasional_income'].empty:
                        all_months.update(st.session_state.data_manager['occasional']['occasional_income']['收入日期'].dt.to_period('M').astype(str))
                    if not st.session_state.data_manager['occasional']['occasional_expense'].empty:
                        all_months.update(st.session_state.data_manager['occasional']['occasional_expense']['支出日期'].dt.to_period('M').astype(str))
                    months_list = sorted(list(all_months))
                    monthly_summary = pd.DataFrame({'月份': months_list})
                    monthly_income['月份'] = monthly_income['支付月份'].dt.to_period('M').astype(str)
                    monthly_summary = monthly_summary.merge(monthly_income[['月份', '金额']], on='月份', how='left').fillna(0)
                    monthly_summary.rename(columns={'金额': '收入'}, inplace=True)
                    if not material_df.empty:
                        material_monthly = material_df.groupby('支出月份')['物料成本'].sum().reset_index()
                        material_monthly.columns = ['月份', '物料成本']
                        monthly_summary = monthly_summary.merge(material_monthly, on='月份', how='left').fillna(0)
                    else: monthly_summary['物料成本'] = 0
                    if not labor_df.empty:
                        labor_monthly = labor_df.groupby('支出月份')['成本金额'].sum().reset_index()
                        labor_monthly.columns = ['月份', '人工成本']
                        monthly_summary = monthly_summary.merge(labor_monthly, on='月份', how='left').fillna(0)
                    else: monthly_summary['人工成本'] = 0
                    if not admin_df.empty:
                        admin_monthly = admin_df.groupby('支出月份')['月度成本'].sum().reset_index()
                        admin_monthly.columns = ['月份', '行政成本']
                        monthly_summary = monthly_summary.merge(admin_monthly, on='月份', how='left').fillna(0)
                    else: monthly_summary['行政成本'] = 0
                    if not st.session_state.data_manager['occasional']['occasional_income'].empty:
                        occasional_income_monthly = st.session_state.data_manager['occasional']['occasional_income'].groupby(st.session_state.data_manager['occasional']['occasional_income']['收入日期'].dt.to_period('M').astype(str))['收入金额'].sum().reset_index()
                        occasional_income_monthly.columns = ['月份', '偶然收入']
                        monthly_summary = monthly_summary.merge(occasional_income_monthly, on='月份', how='left').fillna(0)
                    else: monthly_summary['偶然收入'] = 0
                    if not st.session_state.data_manager['occasional']['occasional_expense'].empty:
                        occasional_expense_monthly = st.session_state.data_manager['occasional']['occasional_expense'].groupby(st.session_state.data_manager['occasional']['occasional_expense']['支出日期'].dt.to_period('M').astype(str))['支出金额'].sum().reset_index()
                        occasional_expense_monthly.columns = ['月份', '偶然支出']
                        monthly_summary = monthly_summary.merge(occasional_expense_monthly, on='月份', how='left').fillna(0)
                    else: monthly_summary['偶然支出'] = 0
                    monthly_summary['净现金流'] = monthly_summary['收入'] + monthly_summary['偶然收入'] - (monthly_summary['物料成本'] + monthly_summary['人工成本'] + monthly_summary['行政成本'] + monthly_summary['偶然支出'])
                    monthly_summary['累计现金余额'] = st.session_state.current_cash_balance
                    for i in range(len(monthly_summary)):
                        if i == 0: monthly_summary.loc[i, '累计现金余额'] = st.session_state.current_cash_balance + monthly_summary.loc[i, '净现金流']
                        else: monthly_summary.loc[i, '累计现金余额'] = monthly_summary.loc[i-1, '累计现金余额'] + monthly_summary.loc[i, '净现金流']
                    runway_months = 0
                    for idx, row in monthly_summary.iterrows():
                        if row['累计现金余额'] <= 0: break
                        runway_months += 1
                    st.metric("当前现金余额", f"{st.session_state.current_cash_balance:.2f} 万元")
                    st.metric("预计Runway", f"{runway_months} 个月")
                    fig_runway = go.Figure()
                    fig_runway.add_trace(go.Scatter(x=monthly_summary['月份'], y=monthly_summary['累计现金余额'], mode='lines+markers', name='累计现金余额', line=dict(color='#1a2a6c', width=3), marker=dict(size=8)))
                    fig_runway.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="现金枯竭")
                    fig_runway.update_layout(title='现金余额趋势', xaxis_title='月份', yaxis_title='累计现金余额 (万元)', hovermode='x unified', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig_runway, use_container_width=True)
                    st.subheader("现金流详情表")
                    runway_display = monthly_summary[['月份', '收入', '物料成本', '人工成本', '行政成本', '偶然收入', '偶然支出', '净现金流', '累计现金余额']].copy()
                    st.dataframe(runway_display.style.format({
                        '收入': '{:.2f}', '物料成本': '{:.2f}', '人工成本': '{:.2f}', '行政成本': '{:.2f}',
                        '偶然收入': '{:.2f}', '偶然支出': '{:.2f}', '净现金流': '{:.2f}', '累计现金余额': '{:.2f}'
                    }), use_container_width=True)
                else: st.info("请在系统配置中设置当前现金余额以进行Runway分析。")
            else: st.info("暂无现金流数据，需要先添加收入预算项目。")
        else: st.info("暂无项目数据。请先添加收入预算项目以进行现金流分析。")

    elif st.session_state.selected_page == "全面预算汇总":
        st.header("📋 全面预算汇总表")
        if not st.session_state.data_manager['income'].data.empty:
            # === 收入汇总 ===
            income_monthly = st.session_state.data_manager['income'].data.copy()
            income_monthly['交付月份'] = pd.to_datetime(income_monthly['交付日期']).dt.to_period('M')
            income_summary = income_monthly.groupby('交付月份')['纠偏后收入'].sum().reset_index()
            income_summary['月份'] = income_summary['交付月份'].astype(str)
    
            # === 物料成本 ===
            material_monthly = st.session_state.data_manager['income'].generate_material_cost_data()
            if not material_monthly.empty:
                material_summary = material_monthly.groupby('支出月份')['物料成本'].sum().reset_index()
            else:
                material_summary = pd.DataFrame({'支出月份': [], '物料成本': []})
    
            # === 人工成本 ===
            labor_monthly = st.session_state.data_manager['labor'].generate_cost_data()
            if not labor_monthly.empty:
                labor_summary = labor_monthly.groupby('支出月份')['成本金额'].sum().reset_index()
            else:
                labor_summary = pd.DataFrame({'支出月份': [], '成本金额': []})
    
            # === 行政费用 ===
            admin_monthly = st.session_state.data_manager['admin'].generate_cost_data()
            if not admin_monthly.empty:
                admin_summary = admin_monthly.groupby('支出月份')['月度成本'].sum().reset_index()
            else:
                admin_summary = pd.DataFrame({'支出月份': [], '月度成本': []})
    
            # === 偶然收入 ===
            if not st.session_state.data_manager['occasional']['occasional_income'].empty:
                df_inc = st.session_state.data_manager['occasional']['occasional_income'].copy()
                df_inc['月份'] = pd.to_datetime(df_inc['收入日期']).dt.to_period('M').astype(str)
                occasional_income_monthly = df_inc.groupby('月份')['收入金额'].sum().reset_index()
                occasional_income_monthly.rename(columns={'收入金额': '偶然收入'}, inplace=True)
            else:
                occasional_income_monthly = pd.DataFrame({'月份': [], '偶然收入': []})
    
            # === 偶然支出 ===
            if not st.session_state.data_manager['occasional']['occasional_expense'].empty:
                df_exp = st.session_state.data_manager['occasional']['occasional_expense'].copy()
                df_exp['月份'] = pd.to_datetime(df_exp['支出日期']).dt.to_period('M').astype(str)
                occasional_expense_monthly = df_exp.groupby('月份')['支出金额'].sum().reset_index()
                occasional_expense_monthly.rename(columns={'支出金额': '偶然支出'}, inplace=True)
            else:
                occasional_expense_monthly = pd.DataFrame({'月份': [], '偶然支出': []})
    
            # === 收集所有月份 ===
            all_months = set()
            if not income_summary.empty:
                all_months.update(income_summary['月份'])
            if not material_summary.empty:
                all_months.update(material_summary['支出月份'])
            if not labor_summary.empty:
                all_months.update(labor_summary['支出月份'])
            if not admin_summary.empty:
                all_months.update(admin_summary['支出月份'])
            if not occasional_income_monthly.empty:
                all_months.update(occasional_income_monthly['月份'])
            if not occasional_expense_monthly.empty:
                all_months.update(occasional_expense_monthly['月份'])
    
            months_list = sorted(list(all_months))
            budget_summary = pd.DataFrame({'月份': months_list})
    
            # === 合并各项数据 ===
            # 收入
            if not income_summary.empty:
                budget_summary = budget_summary.merge(
                    income_summary[['月份', '纠偏后收入']], on='月份', how='left'
                )
            else:
                budget_summary['纠偏后收入'] = 0
    
            # 物料成本
            if not material_summary.empty:
                budget_summary = budget_summary.merge(
                    material_summary[['支出月份', '物料成本']],
                    left_on='月份', right_on='支出月份', how='left'
                ).drop(columns=['支出月份'])
            else:
                budget_summary['物料成本'] = 0
    
            # 人工成本
            if not labor_summary.empty:
                budget_summary = budget_summary.merge(
                    labor_summary[['支出月份', '成本金额']],
                    left_on='月份', right_on='支出月份', how='left'
                ).drop(columns=['支出月份'])
            else:
                budget_summary['成本金额'] = 0
    
            # 行政费用
            if not admin_summary.empty:
                budget_summary = budget_summary.merge(
                    admin_summary[['支出月份', '月度成本']],
                    left_on='月份', right_on='支出月份', how='left'
                ).drop(columns=['支出月份'])
            else:
                budget_summary['月度成本'] = 0
    
            # 偶然收入
            if not occasional_income_monthly.empty and '偶然收入' in occasional_income_monthly.columns:
                budget_summary = budget_summary.merge(
                    occasional_income_monthly[['月份', '偶然收入']], on='月份', how='left'
                )
            else:
                budget_summary['偶然收入'] = 0
    
            # 偶然支出
            if not occasional_expense_monthly.empty and '偶然支出' in occasional_expense_monthly.columns:
                budget_summary = budget_summary.merge(
                    occasional_expense_monthly[['月份', '偶然支出']], on='月份', how='left'
                )
            else:
                budget_summary['偶然支出'] = 0
    
            # 统一填充缺失值
            budget_summary = budget_summary.fillna(0)
    
            # === 计算衍生指标 ===
            budget_summary['总收入'] = budget_summary['纠偏后收入'] + budget_summary['偶然收入']
            budget_summary['总支出'] = (
                budget_summary['物料成本'] +
                budget_summary['成本金额'] +
                budget_summary['月度成本'] +
                budget_summary['偶然支出']
            )
            budget_summary['毛利润'] = budget_summary['总收入'] - budget_summary['总支出']
            budget_summary['毛利率'] = np.where(
                budget_summary['总收入'] > 0,
                budget_summary['毛利润'] / budget_summary['总收入'] * 100,
                0
            )
    
            # === 排序与格式化月份 ===
            budget_summary['月份_dt'] = pd.to_datetime(budget_summary['月份'])
            budget_summary = budget_summary.sort_values('月份_dt').drop(columns=['月份_dt'])
            budget_summary['月份_中文'] = pd.to_datetime(budget_summary['月份']).apply(
                lambda x: f"{x.year}年{x.month}月"
            )
            budget_summary = budget_summary.rename(columns={'月份': '月份_英文', '月份_中文': '月份'})
    
            # === 显示表格 ===
            st.subheader("月度预算汇总")
            budget_display = budget_summary.copy()
            budget_display = budget_display.rename(columns={'月份': '月份_中文'})
            st.dataframe(
                budget_display.style.format({
                    '纠偏后收入': '{:.2f}', '物料成本': '{:.2f}', '成本金额': '{:.2f}',
                    '月度成本': '{:.2f}', '偶然收入': '{:.2f}', '偶然支出': '{:.2f}',
                    '总收入': '{:.2f}', '总支出': '{:.2f}', '毛利润': '{:.2f}',
                    '毛利率': '{:.2f}%'
                }),
                use_container_width=True
            )
    
            # === 汇总统计 ===
            total_income = budget_summary['总收入'].sum()
            total_material = budget_summary['物料成本'].sum()
            total_labor = budget_summary['成本金额'].sum()
            total_admin = budget_summary['月度成本'].sum()
            total_occasional_income = budget_summary['偶然收入'].sum()
            total_occasional_expense = budget_summary['偶然支出'].sum()
            total_expense = budget_summary['总支出'].sum()
            total_profit = budget_summary['毛利润'].sum()
            avg_margin = (total_profit / total_income * 100) if total_income > 0 else 0
    
            st.subheader("预算汇总统计")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("总收入", f"{total_income:.2f} 万元")
                st.metric("总支出", f"{total_expense:.2f} 万元")
            with col2:
                st.metric("物料成本", f"{total_material:.2f} 万元")
                st.metric("人工成本", f"{total_labor:.2f} 万元")
            with col3:
                st.metric("行政费用", f"{total_admin:.2f} 万元")
                st.metric("偶然收入", f"{total_occasional_income:.2f} 万元")
            with col4:
                st.metric("偶然支出", f"{total_occasional_expense:.2f} 万元")
                st.metric("毛利润", f"{total_profit:.2f} 万元")
                st.metric("平均毛利率", f"{avg_margin:.2f}%")
    
            # === 可视化 ===
            st.subheader("全面预算可视化")
            fig_budget = go.Figure()
            fig_budget.add_trace(go.Bar(x=budget_summary['月份'], y=budget_summary['总收入'], name='总收入', marker_color='#1a2a6c'))
            fig_budget.add_trace(go.Bar(x=budget_summary['月份'], y=budget_summary['物料成本'], name='物料成本', marker_color='#ff6b6b'))
            fig_budget.add_trace(go.Bar(x=budget_summary['月份'], y=budget_summary['成本金额'], name='人工成本', marker_color='#4ecdc4'))
            fig_budget.add_trace(go.Bar(x=budget_summary['月份'], y=budget_summary['月度成本'], name='行政费用', marker_color='#f7b731'))
            fig_budget.add_trace(go.Bar(x=budget_summary['月份'], y=budget_summary['偶然支出'], name='偶然支出', marker_color='#ff9f1c'))
            fig_budget.update_layout(
                title='月度收入与支出对比',
                xaxis_title='月份',
                yaxis_title='金额 (万元)',
                barmode='group',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_budget, use_container_width=True)
    
            fig_margin = go.Figure()
            fig_margin.add_trace(go.Scatter(
                x=budget_summary['月份'],
                y=budget_summary['毛利率'],
                mode='lines+markers',
                name='毛利率',
                line=dict(color='#1a2a6c', width=3),
                marker=dict(size=8)
            ))
            fig_margin.update_layout(
                title='月度毛利率趋势',
                xaxis_title='月份',
                yaxis_title='毛利率 (%)',
                yaxis_range=[-100, 100],
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_margin, use_container_width=True)
    
        else:
            st.info("暂无收入数据。请先添加收入预算项目以生成全面预算汇总表。")

    st.header("❓ 模型说明")
    with st.expander("点击展开查看详细说明"):
        st.markdown("""
        ### 双重风险预测模型
        
        **核心公式**: `纠偏后收入 = 直接输入（无需计算系数）`
        
        #### 1. 保守成单率
        - 取销售提供概率区间的下限值（如50%-80%取50%）
        - 体现研发思维中的保守原则
        
        #### 2. 时间衰减因子
        - 采用指数衰减模型: `e^(-λt)`
        - `λ` = 衰减系数（行业基准0.0315）
        - `t` = 项目交付月份与当前月份的差值（月）
        - **理论依据**：风险随时间呈非线性累积，符合复杂系统不确定性增长规律
        
        #### 3. 手动纠偏
        - 直接输入最终的纠偏后收入金额
        - 无需计算系数，简化操作流程
        - **应用场景**：对确定性极高的项目（如已签约）直接调整金额，对风险较大的项目进行下调
        
        ### 现金流计算逻辑
        - **首付款**：交付当月支付（默认50%，可调整）
        - **次付款**：交付次月支付（默认40%，可调整）
        - **质保金**：交付一年后支付（默认10%，可调整）
        - **现金流预测**：基于纠偏后收入和个性化付款比例计算
        
        ### 付款比例管理
        - 每个项目可设置独立的付款比例
        - 默认比例：50% + 40% + 10% = 100%
        - 系统自动验证比例总和为100%
        - 支持在项目列表中批量调整
        
        ### 物料支出计算逻辑
        - **光谱设备/服务**：默认30%，可手动调整
        - **配液设备**：默认35%，可手动调整
        - **自动化项目**：默认40%，可手动调整
        - **支出时间**：交付月份的前一个月
        - **支出金额**：纠偏后收入 × 物料支出比例
        
        ### 人工成本管理
        - **成本类型**：销售费用、制造费用、研发费用、管理费用等
        - **人员/部门**：具体的人力资源分配
        - **月度成本**：每月的人工成本
        - **时间范围**：成本生效的时间段
        - **自动计算**：按天数比例分配跨月成本
        
        ### 行政费用管理
        - **费用类型**：房租、水电、办公用品、差旅等
        - **费用项目**：具体的费用项目
        - **付款频率**：月度、季度、年度
        - **时间范围**：费用生效的时间段
        
        ### 偶然收支管理
        - **偶然收入**：政府补贴、投资收益、一次性收入等
        - **偶然支出**：罚款、维修、捐赠等一次性支出
        - **核算方式**：计入月度现金流，影响Runway分析
        
        ### Runway分析
        - **现金余额**：在系统配置中设置当前现金余额
        - **净现金流**：月度收入 - 月度支出
        - **Runway计算**：累计现金余额首次为负的月份
        - **趋势图**：显示现金余额随时间的变化趋势
        
        ### 战略价值
        1. **数据驱动决策**：取代经验主义，用数学模型量化不确定性
        2. **风险前置管理**：提前识别远期项目的风险，优化资源配置
        3. **模型持续进化**：随着项目数据积累，不断优化衰减系数
        4. **灵活调整**：通过直接输入金额进行灵活调整
        5. **现金流管理**：准确预测资金流入时间，优化资金安排
        6. **个性化管理**：支持每个项目独立的付款比例设置
        7. **Runway分析**：基于当前现金余额预测公司运营时间
        8. **透明化决策**：所有假设和计算过程清晰可见，便于团队共识
        
        ### 系统使用建议
        - **日常更新**：销售每周更新项目状态
        - **月度校准**：每月对比预测vs实际，调整λ参数
        - **预算编制**：使用纠偏后收入总和作为基准
        - **现金流管理**：基于现金流预测安排资金计划
        - **Runway监控**：定期更新现金余额，监控公司Runway
        - **挑战目标**：基准值+20%
        """)

    st.markdown("---")
    st.markdown("<div style='text-align: center; color: #666666; padding: 20px;'>全面预算管理系统 © 2025 | 咸数科技 · 财务小王 | 当前版本: 3.5</div>", unsafe_allow_html=True)

    if 'first_run' not in st.session_state:
        st.session_state.first_run = True
        st.toast("全面预算管理系统已就绪！您可以通过手动添加或一键导入开始预算编制。", icon="✅")


if __name__ == "__main__":
    main()




















