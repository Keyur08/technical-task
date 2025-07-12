import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Optional, List, Dict
from datetime import date
import logging
import numpy as np  
from database.operations import DatabaseOperations

logger = logging.getLogger(__name__)

class DataVisualizer:
    def __init__(self, db_ops: DatabaseOperations):
        self.db_ops = db_ops
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
    
    def read_data_to_dataframe(self, 
                              start_date: Optional[date] = None,
                              end_date: Optional[date] = None,
                              fuel_types: Optional[List[str]] = None) -> pd.DataFrame:
        """Read data from database and return as pandas DataFrame."""
        try:
            records = self.db_ops.get_data(start_date, end_date, fuel_types)
            
            if not records:
                logger.warning("No data found")
                return pd.DataFrame()
            
            data = []
            for record in records:
                data.append({
                    'settlement_date': record.settlement_date,
                    'settlement_period': record.settlement_period,
                    'psr_type': record.psr_type,
                    'quantity': float(record.quantity) if record.quantity else 0,
                    'fuel_type': record.fuel_type,
                    'region': record.region,
                    'publish_time': record.publish_time,
                    'start_time': record.start_time
                })
            
            df = pd.DataFrame(data)
            logger.info(f"Loaded {len(df)} records into DataFrame")
            return df
            
        except Exception as e:
            logger.error(f"Error reading data to DataFrame: {e}")
            raise
    
    def create_daily_generation_plot(self, 
                                   start_date: Optional[date] = None,
                                   end_date: Optional[date] = None,
                                   save_path: Optional[str] = None,
                                   title: Optional[str] = None) -> plt.Figure:
        """Create daily generation plot by fuel type."""
        df = self.read_data_to_dataframe(start_date, end_date)
        
        if df.empty:
            raise ValueError("No data available for plotting")
        
        daily_data = df.groupby(['settlement_date', 'psr_type'])['quantity'].sum().reset_index()
        pivot_data = daily_data.pivot(index='settlement_date', columns='psr_type', values='quantity')
        pivot_data = pivot_data.fillna(0)
        
        fig, ax = plt.subplots(figsize=(15, 8))
        pivot_data.plot(kind='area', stacked=True, ax=ax, alpha=0.7)
        
        plot_title = title or 'Daily Wind & Solar Generation by Type'
        ax.set_title(plot_title, fontsize=16, fontweight='bold')
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Generation (MWh)', fontsize=12)
        ax.legend(title='Fuel Type', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Plot saved to {save_path}")
        
        return fig
    
    def create_monthly_comparison_plot(self, 
                                 start_date: Optional[date] = None,
                                 end_date: Optional[date] = None,
                                 save_path: Optional[str] = None,
                                 title: Optional[str] = None) -> plt.Figure:
        """Create monthly comparison plot."""
        df = self.read_data_to_dataframe(start_date, end_date)
        
        if df.empty:
            raise ValueError("No data available for plotting")
        
        df['settlement_date'] = pd.to_datetime(df['settlement_date'])
        df['month'] = df['settlement_date'].dt.to_period('M')
        monthly_data = df.groupby(['month', 'psr_type'])['quantity'].sum().reset_index()
        
        fig, ax = plt.subplots(figsize=(15, 8))
        sns.barplot(data=monthly_data, x='month', y='quantity', hue='psr_type', ax=ax)
        
        plot_title = title or 'Monthly Wind & Solar Generation Comparison'
        ax.set_title(plot_title, fontsize=16, fontweight='bold')
        ax.set_xlabel('Month', fontsize=12)
        ax.set_ylabel('Generation (MWh)', fontsize=12)
        ax.legend(title='Fuel Type')
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def create_settlement_period_heatmap(self, 
                              start_date: Optional[date] = None,
                              end_date: Optional[date] = None,
                              fuel_type: str = None,
                              save_path: Optional[str] = None,
                              title: Optional[str] = None) -> plt.Figure:
        """Create heatmap showing generation patterns by settlement period."""
        df = self.read_data_to_dataframe(start_date, end_date, [fuel_type] if fuel_type else None)
        
        if df.empty:
            raise ValueError("No data available for plotting")
        
        if fuel_type:
            df = df[df['psr_type'].str.lower() == fuel_type.lower()]
        
        pivot_data = df.pivot_table(
            index='settlement_date', 
            columns='settlement_period', 
            values='quantity', 
            aggfunc='sum'
        ).fillna(0)
        
        fig, ax = plt.subplots(figsize=(20, 10))
        sns.heatmap(pivot_data, cmap='YlOrRd', ax=ax, cbar_kws={'label': 'Generation (MWh)'})
        
        plot_title = title or f'Generation Heatmap by Settlement Period'
        if fuel_type:
            plot_title += f' - {fuel_type}'
        
        ax.set_title(plot_title, fontsize=16, fontweight='bold')
        ax.set_xlabel('Settlement Period', fontsize=12)
        ax.set_ylabel('Date', fontsize=12)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def create_fuel_comparison_plot(self,
                                  start_date: Optional[date] = None,
                                  end_date: Optional[date] = None,
                                  save_path: Optional[str] = None,
                                  title: Optional[str] = None) -> plt.Figure:
        """Create fuel type comparison plot."""
        df = self.read_data_to_dataframe(start_date, end_date)
        
        if df.empty:
            raise ValueError("No data available for plotting")
        
        fuel_totals = df.groupby('psr_type')['quantity'].sum().sort_values(ascending=True)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Bar chart
        fuel_totals.plot(kind='barh', ax=ax1)
        ax1.set_title('Total Generation by Fuel Type')
        ax1.set_xlabel('Total Generation (MWh)')
        
        # Pie chart
        ax2.pie(fuel_totals.values, labels=fuel_totals.index, autopct='%1.1f%%')
        ax2.set_title('Generation Share by Fuel Type')
        
        if title:
            fig.suptitle(title, fontsize=16, fontweight='bold')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def generate_summary_report(self, 
                              start_date: Optional[date] = None,
                              end_date: Optional[date] = None) -> Dict:
        """Generate comprehensive summary report."""
        df = self.read_data_to_dataframe(start_date, end_date)
        
        if df.empty:
            return {"status": "error", "message": "No data available"}
        
        summary = {
            "total_records": len(df),
            "date_range": {
                "start": str(df['settlement_date'].min()),
                "end": str(df['settlement_date'].max()),
                "days": (df['settlement_date'].max() - df['settlement_date'].min()).days + 1
            },
            "fuel_type_stats": {},
            "daily_stats": {
                "avg_daily_generation": df.groupby('settlement_date')['quantity'].sum().mean(),
                "max_daily_generation": df.groupby('settlement_date')['quantity'].sum().max(),
                "min_daily_generation": df.groupby('settlement_date')['quantity'].sum().min()
            }
        }
        
        for fuel_type in df['psr_type'].unique():
            fuel_data = df[df['psr_type'] == fuel_type]
            summary["fuel_type_stats"][fuel_type] = {
                "total_generation": fuel_data['quantity'].sum(),
                "avg_generation": fuel_data['quantity'].mean(),
                "max_generation": fuel_data['quantity'].max(),
                "record_count": len(fuel_data)
            }
        
        return {"status": "success", "summary": summary}