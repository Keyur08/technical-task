# tests/test_visualization.py
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date
from utils.visualization import DataVisualizer
from database.operations import DatabaseOperations
from database.models import WindSolarGeneration


class TestDataVisualizer:
    """Test cases for DataVisualizer class."""
    
    @pytest.fixture
    def mock_db_ops(self):
        """Create mock database operations."""
        return Mock(spec=DatabaseOperations)
    
    @pytest.fixture
    def visualizer(self, mock_db_ops):
        """Create DataVisualizer instance with mock database operations."""
        return DataVisualizer(mock_db_ops)
    
    @pytest.fixture
    def sample_db_records(self):
        """Create sample database records for testing."""
        records = []
        for i in range(3):
            record = Mock(spec=WindSolarGeneration)
            record.settlement_date = date(2024, 1, i + 1)
            record.settlement_period = 1
            record.psr_type = "Solar" if i % 2 == 0 else "Wind Onshore"
            record.quantity = 100.0 + i * 50
            record.fuel_type = record.psr_type
            record.region = "GB"
            record.publish_time = None
            record.start_time = None
            records.append(record)
        return records
    
    def test_read_data_to_dataframe_success(self, visualizer, mock_db_ops, sample_db_records):
        """Test successful conversion of database records to DataFrame."""
        mock_db_ops.get_data.return_value = sample_db_records
        
        df = visualizer.read_data_to_dataframe()
        
        assert len(df) == 3
        assert list(df.columns) == [
            'settlement_date', 'settlement_period', 'psr_type', 'quantity',
            'fuel_type', 'region', 'publish_time', 'start_time'
        ]
        assert df['psr_type'].iloc[0] == "Solar"
        assert df['quantity'].iloc[0] == 100.0
        mock_db_ops.get_data.assert_called_once_with(None, None, None)
    
    def test_read_data_to_dataframe_with_filters(self, visualizer, mock_db_ops):
        """Test DataFrame creation with date and fuel type filters."""
        mock_db_ops.get_data.return_value = []
        
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)
        fuel_types = ["Solar", "Wind Onshore"]
        
        df = visualizer.read_data_to_dataframe(start_date, end_date, fuel_types)
        
        assert df.empty
        mock_db_ops.get_data.assert_called_once_with(start_date, end_date, fuel_types)
    
    def test_read_data_to_dataframe_empty_result(self, visualizer, mock_db_ops):
        """Test DataFrame creation with no data."""
        mock_db_ops.get_data.return_value = []
        
        df = visualizer.read_data_to_dataframe()
        
        assert df.empty
        assert isinstance(df, pd.DataFrame)
    
    def test_read_data_to_dataframe_error_handling(self, visualizer, mock_db_ops):
        """Test error handling in DataFrame creation."""
        mock_db_ops.get_data.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            visualizer.read_data_to_dataframe()
    
    @patch('utils.visualization.plt')
    def test_create_daily_generation_plot_success(self, mock_plt, visualizer, mock_db_ops, sample_db_records):
        """Test successful daily generation plot creation."""
        mock_db_ops.get_data.return_value = sample_db_records
        
        # Mock matplotlib components
        mock_fig = Mock()
        mock_ax = Mock()
        mock_plt.subplots.return_value = (mock_fig, mock_ax)
        
        # Mock pandas plot method
        with patch.object(pd.DataFrame, 'plot') as mock_plot:
            result = visualizer.create_daily_generation_plot()
        
        assert result == mock_fig
        mock_plt.subplots.assert_called_once_with(figsize=(15, 8))
        mock_ax.set_title.assert_called_once()
        mock_ax.set_xlabel.assert_called_once_with('Date', fontsize=12)
        mock_ax.set_ylabel.assert_called_once_with('Generation (MWh)', fontsize=12)
        mock_plt.tight_layout.assert_called_once()
    
    @patch('utils.visualization.plt')
    def test_create_daily_generation_plot_with_save(self, mock_plt, visualizer, mock_db_ops, sample_db_records):
        """Test daily generation plot with save functionality."""
        mock_db_ops.get_data.return_value = sample_db_records
        
        mock_fig = Mock()
        mock_ax = Mock()
        mock_plt.subplots.return_value = (mock_fig, mock_ax)
        
        save_path = "/tmp/test_plot.png"
        
        with patch.object(pd.DataFrame, 'plot'):
            visualizer.create_daily_generation_plot(save_path=save_path)
        
        mock_plt.savefig.assert_called_once_with(save_path, dpi=300, bbox_inches='tight')
    
    def test_create_daily_generation_plot_no_data(self, visualizer, mock_db_ops):
        """Test daily generation plot with no data raises error."""
        mock_db_ops.get_data.return_value = []
        
        with pytest.raises(ValueError, match="No data available for plotting"):
            visualizer.create_daily_generation_plot()
    
    @patch('utils.visualization.plt')
    @patch('utils.visualization.sns')
    def test_create_monthly_comparison_plot(self, mock_sns, mock_plt, visualizer, mock_db_ops, sample_db_records):
        """Test monthly comparison plot creation."""
        mock_db_ops.get_data.return_value = sample_db_records
        
        mock_fig = Mock()
        mock_ax = Mock()
        mock_plt.subplots.return_value = (mock_fig, mock_ax)
        
        result = visualizer.create_monthly_comparison_plot()
        
        assert result == mock_fig
        mock_sns.barplot.assert_called_once()
        mock_ax.set_title.assert_called_once()
        mock_plt.xticks.assert_called_once_with(rotation=45)
    
    @patch('utils.visualization.plt')
    @patch('utils.visualization.sns')
    def test_create_settlement_period_heatmap(self, mock_sns, mock_plt, visualizer, mock_db_ops, sample_db_records):
        """Test settlement period heatmap creation."""
        mock_db_ops.get_data.return_value = sample_db_records
        
        mock_fig = Mock()
        mock_ax = Mock()
        mock_plt.subplots.return_value = (mock_fig, mock_ax)
        
        result = visualizer.create_settlement_period_heatmap(fuel_type="Solar")
        
        assert result == mock_fig
        mock_sns.heatmap.assert_called_once()
        mock_ax.set_title.assert_called_once()
        mock_ax.set_xlabel.assert_called_once_with('Settlement Period', fontsize=12)
        mock_ax.set_ylabel.assert_called_once_with('Date', fontsize=12)
    
    @patch('utils.visualization.plt')
    def test_create_fuel_comparison_plot(self, mock_plt, visualizer, mock_db_ops, sample_db_records):
        """Test fuel comparison plot creation."""
        mock_db_ops.get_data.return_value = sample_db_records
        
        mock_fig = Mock()
        mock_ax1 = Mock()
        mock_ax2 = Mock()
        mock_plt.subplots.return_value = (mock_fig, [mock_ax1, mock_ax2])
        
        # Mock pandas plot and pie methods
        with patch.object(pd.Series, 'plot') as mock_plot:
            result = visualizer.create_fuel_comparison_plot()
        
        assert result == mock_fig
        mock_ax1.set_title.assert_called_once_with('Total Generation by Fuel Type')
        mock_ax1.set_xlabel.assert_called_once_with('Total Generation (MWh)')
        mock_ax2.pie.assert_called_once()
        mock_ax2.set_title.assert_called_once_with('Generation Share by Fuel Type')
    
    def test_generate_summary_report_success(self, visualizer, mock_db_ops, sample_db_records):
        """Test successful summary report generation."""
        mock_db_ops.get_data.return_value = sample_db_records
        
        result = visualizer.generate_summary_report()
        
        assert result["status"] == "success"
        assert "summary" in result
        summary = result["summary"]
        assert summary["total_records"] == 3
        assert "date_range" in summary
        assert "fuel_type_stats" in summary
        assert "daily_stats" in summary
        
        # Check fuel type stats
        assert "Solar" in summary["fuel_type_stats"]
        assert "Wind Onshore" in summary["fuel_type_stats"]
        
        # Check solar stats
        solar_stats = summary["fuel_type_stats"]["Solar"]
        assert "total_generation" in solar_stats
        assert "avg_generation" in solar_stats
        assert "max_generation" in solar_stats
        assert "record_count" in solar_stats
    
    def test_generate_summary_report_no_data(self, visualizer, mock_db_ops):
        """Test summary report with no data."""
        mock_db_ops.get_data.return_value = []
        
        result = visualizer.generate_summary_report()
        
        assert result["status"] == "error"
        assert "No data available" in result["message"]
    
    def test_generate_summary_report_with_date_filter(self, visualizer, mock_db_ops, sample_db_records):
        """Test summary report with date filters."""
        mock_db_ops.get_data.return_value = sample_db_records
        
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)
        
        result = visualizer.generate_summary_report(start_date, end_date)
        
        assert result["status"] == "success"
        mock_db_ops.get_data.assert_called_once_with(start_date, end_date, None)


class TestDataVisualizerIntegration:
    """Integration tests for DataVisualizer."""
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            'settlement_date': [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            'settlement_period': [1, 1, 1],
            'psr_type': ['Solar', 'Wind Onshore', 'Solar'],
            'quantity': [100.0, 150.0, 120.0],
            'fuel_type': ['Solar', 'Wind Onshore', 'Solar'],
            'region': ['GB', 'GB', 'GB'],
            'publish_time': [None, None, None],
            'start_time': [None, None, None]
        })
    
    def test_data_processing_for_daily_plot(self, sample_dataframe):
        """Test data processing logic for daily plots."""
        # Group by date and fuel type (similar to what's done in create_daily_generation_plot)
        daily_data = sample_dataframe.groupby(['settlement_date', 'psr_type'])['quantity'].sum().reset_index()
        pivot_data = daily_data.pivot(index='settlement_date', columns='psr_type', values='quantity')
        pivot_data = pivot_data.fillna(0)
        
        assert len(pivot_data) == 3  # 3 dates
        assert 'Solar' in pivot_data.columns
        assert 'Wind Onshore' in pivot_data.columns
        assert pivot_data.loc[date(2024, 1, 1), 'Solar'] == 100.0
        assert pivot_data.loc[date(2024, 1, 2), 'Wind Onshore'] == 150.0
        assert pivot_data.loc[date(2024, 1, 3), 'Solar'] == 120.0
    
    def test_data_processing_for_monthly_plot(self, sample_dataframe):
        """Test data processing logic for monthly plots."""
        df = sample_dataframe.copy()
        df['settlement_date'] = pd.to_datetime(df['settlement_date'])
        df['month'] = df['settlement_date'].dt.to_period('M')
        monthly_data = df.groupby(['month', 'psr_type'])['quantity'].sum().reset_index()
        
        assert len(monthly_data) == 2  # Solar and Wind Onshore for January 2024
        assert monthly_data[monthly_data['psr_type'] == 'Solar']['quantity'].iloc[0] == 220.0  # 100 + 120
        assert monthly_data[monthly_data['psr_type'] == 'Wind Onshore']['quantity'].iloc[0] == 150.0
    
    def test_data_processing_for_fuel_comparison(self, sample_dataframe):
        """Test data processing logic for fuel comparison plots."""
        fuel_totals = sample_dataframe.groupby('psr_type')['quantity'].sum().sort_values(ascending=True)
        
        assert len(fuel_totals) == 2
        assert fuel_totals['Wind Onshore'] == 150.0
        assert fuel_totals['Solar'] == 220.0  # 100 + 120
        assert fuel_totals.index.tolist() == ['Wind Onshore', 'Solar']  # Sorted ascending


if __name__ == "__main__":
    # pytest tests/test_visualization.py -v
    pass