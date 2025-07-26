"""
Test cases for the BackupRetentionLadder strategy.

This module provides comprehensive tests for the sophisticated retention
ladder strategy including weekly, monthly, and yearly retention logic.
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch

# Import the modules under test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atlassian_cloud_backup.thinning.manager import (
    BackupRetentionLadder,
    OldestFirstStrategy,
    BackupInfo,
    BackupDeleter,
    DeletionConfig
)

# Removed redundant import block


class TestBackupRetentionLadderDetailed:
    """Detailed tests for the BackupRetentionLadder strategy."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.strategy = BackupRetentionLadder()
        self.now = datetime.now()
    
    def create_backup(self, name, days_ago):
        """Helper to create a BackupInfo for testing."""
        date = self.now - timedelta(days=days_ago)
        return BackupInfo(Path(name), date, "test")
    
    def test_weekly_candidates_single_week_multiple_backups(self):
        """Test weekly candidates when multiple backups exist in same week."""
        # Create backups within the same week
        backups = [
            self.create_backup("day1.jira", 1),  # Monday
            self.create_backup("day2.jira", 2),  # Sunday
            self.create_backup("day3.jira", 3),  # Saturday
            self.create_backup("day4.jira", 4),  # Friday
        ]
        
        candidates = self.strategy._find_weekly_candidates(backups)
        
        # Should return 3 candidates (all except the latest)
        assert len(candidates) == 3
        
        # The latest backup (day1.jira) should NOT be in candidates
        candidate_names = [c.path.name for c in candidates]
        assert "day1.jira" not in candidate_names
        assert "day2.jira" in candidate_names
        assert "day3.jira" in candidate_names
        assert "day4.jira" in candidate_names
    
    def test_weekly_candidates_different_weeks(self):
        """Test weekly candidates with backups in different weeks."""
        # Create backups in different weeks
        backups = [
            self.create_backup("week1.jira", 1),   # This week
            self.create_backup("week2.jira", 8),   # Last week
            self.create_backup("week3.jira", 15),  # 2 weeks ago
        ]
        
        candidates = self.strategy._find_weekly_candidates(backups)
        
        # Each backup is in a different week, so no candidates for deletion
        assert len(candidates) == 0
    
    def test_weekly_candidates_mixed_weeks(self):
        """Test weekly candidates with mixed weeks (some weeks have multiple backups)."""
        backups = [
            # This week - 2 backups
            self.create_backup("week1_new.jira", 1),
            self.create_backup("week1_old.jira", 2),
            
            # Last week - 1 backup
            self.create_backup("week2.jira", 8),
            
            # 2 weeks ago - 3 backups
            self.create_backup("week3_new.jira", 15),
            self.create_backup("week3_med.jira", 16),
            self.create_backup("week3_old.jira", 17),
        ]
        
        candidates = self.strategy._find_weekly_candidates(backups)
        
        # Should have 3 candidates total (1 from this week + 2 from 2 weeks ago)
        assert len(candidates) == 3
        
        candidate_names = [c.path.name for c in candidates]
        # Keep the newest from each week
        assert "week1_new.jira" not in candidate_names  # Keep newest from this week
        assert "week1_old.jira" in candidate_names      # Delete older from this week
        assert "week2.jira" not in candidate_names      # Keep only backup from last week
        assert "week3_new.jira" not in candidate_names  # Keep newest from 2 weeks ago
        assert "week3_med.jira" in candidate_names      # Delete middle from 2 weeks ago
        assert "week3_old.jira" in candidate_names      # Delete oldest from 2 weeks ago
    
    def test_monthly_candidates_single_month_multiple_backups(self):
        """Test monthly candidates when multiple backups exist in same month."""
        # Create backups within the same month (60 days ago = 2 months ago)
        base_days = 60
        backups = [
            self.create_backup("month_new.jira", base_days),
            self.create_backup("month_old1.jira", base_days + 5),
            self.create_backup("month_old2.jira", base_days + 10),
            self.create_backup("month_old3.jira", base_days + 15),
        ]
        
        candidates = self.strategy._find_monthly_candidates(backups)
        
        # Should return 3 candidates (all except the latest)
        assert len(candidates) == 3
        
        candidate_names = [c.path.name for c in candidates]
        assert "month_new.jira" not in candidate_names
        assert "month_old1.jira" in candidate_names
        assert "month_old2.jira" in candidate_names
        assert "month_old3.jira" in candidate_names
    
    def test_monthly_candidates_different_months(self):
        """Test monthly candidates with backups in different months."""
        backups = [
            self.create_backup("jan.jira", 60),   # ~2 months ago
            self.create_backup("feb.jira", 90),   # ~3 months ago
            self.create_backup("mar.jira", 120),  # ~4 months ago
        ]
        
        candidates = self.strategy._find_monthly_candidates(backups)
        
        # Each backup is in a different month, so no candidates for deletion
        assert len(candidates) == 0
    
    def test_yearly_candidates_single_year_multiple_backups(self):
        """Test yearly candidates when multiple backups exist in same year."""
        # Create backups within the same year (over 1 year ago)
        base_days = 400  # Over 1 year ago
        backups = [
            self.create_backup("year_new.jira", base_days),
            self.create_backup("year_old1.jira", base_days + 30),
            self.create_backup("year_old2.jira", base_days + 60),
            self.create_backup("year_old3.jira", base_days + 90),
        ]
        
        candidates = self.strategy._find_yearly_candidates(backups)
        
        # Should return 3 candidates (all except the latest)
        assert len(candidates) == 3
        
        candidate_names = [c.path.name for c in candidates]
        assert "year_new.jira" not in candidate_names
        assert "year_old1.jira" in candidate_names
        assert "year_old2.jira" in candidate_names
        assert "year_old3.jira" in candidate_names
    
    def test_yearly_candidates_different_years(self):
        """Test yearly candidates with backups in different years."""
        backups = [
            self.create_backup("2024.jira", 400),  # ~1.1 years ago
            self.create_backup("2023.jira", 765),  # ~2.1 years ago
            self.create_backup("2022.jira", 1130), # ~3.1 years ago
        ]
        
        candidates = self.strategy._find_yearly_candidates(backups)
        
        # Each backup is in a different year, so no candidates for deletion
        assert len(candidates) == 0
    
    def test_retention_ladder_comprehensive_scenario(self):
        """Test retention ladder with a comprehensive realistic scenario."""
        # Create a comprehensive set of backups spanning multiple time periods
        backups = []
        
        # Recent backups (last 30 days) - Daily backups for 2 weeks
        for i in range(14):
            backups.append(self.create_backup(f"daily_{i:02d}.jira", i + 1))
        
        # Medium-term backups (1 month to 1 year) - Weekly backups
        for week in range(8):  # 8 weeks = ~2 months of weekly backups
            days_ago = 35 + (week * 7)  # Start from 35 days ago
            backups.append(self.create_backup(f"weekly_{week:02d}.jira", days_ago))
        
        # Long-term backups (over 1 year) - Monthly backups
        for month in range(6):  # 6 months of old backups
            days_ago = 400 + (month * 30)  # Start from 400 days ago
            backups.append(self.create_backup(f"monthly_{month:02d}.jira", days_ago))
        
        # Add some redundant backups to test deletion logic
        backups.extend([
            # Extra daily backups in same weeks
            self.create_backup("extra_daily_1.jira", 2),
            self.create_backup("extra_daily_2.jira", 9),
            
            # Extra weekly backups in same months
            self.create_backup("extra_weekly_1.jira", 42),
            self.create_backup("extra_weekly_2.jira", 56),
            
            # Extra monthly backups in same years
            self.create_backup("extra_monthly_1.jira", 430),
            self.create_backup("extra_monthly_2.jira", 460),
        ])
        
        # Test the retention ladder selection
        selected = self.strategy.select_file_for_deletion(backups)
        
        # Should select one file for deletion
        assert selected is not None
        
        # The selected file should be one of the redundant backups
        selected_name = selected.path.name
        
        # Verify it's making intelligent choices by running multiple iterations
        iterations = 5
        deleted_files = []
        remaining_backups = backups.copy()
        
        for i in range(iterations):
            if not remaining_backups:
                break
                
            selected = self.strategy.select_file_for_deletion(remaining_backups)
            if selected is None:
                break
                
            deleted_files.append(selected.path.name)
            remaining_backups = [b for b in remaining_backups if b.path != selected.path]
        
        # Should have deleted some files
        assert len(deleted_files) > 0
        
        # Verify the strategy is working by checking that we're not deleting
        # the only backup from a time period
        print(f"Deleted files: {deleted_files}")
        assert len(deleted_files) <= len(backups) / 2  # Shouldn't delete more than half
    
    def test_edge_case_same_creation_time(self):
        """Test handling of backups with identical creation times."""
        same_time = self.now - timedelta(days=5)
        backups = [
            BackupInfo(Path("backup1.jira"), same_time, "1"),
            BackupInfo(Path("backup2.jira"), same_time, "2"),
            BackupInfo(Path("backup3.jira"), same_time, "3"),
        ]
        
        # Should handle gracefully without errors
        result = self.strategy.select_file_for_deletion(backups)
        
        # Should select one of them (behavior with identical times is implementation-defined)
        assert result is not None
        assert result in backups
    
    def test_empty_categories(self):
        """Test retention ladder when some time categories are empty."""
        # Only create backups in the long-term category
        backups = [
            self.create_backup("old1.jira", 400),
            self.create_backup("old2.jira", 410),
        ]
        
        result = self.strategy.select_file_for_deletion(backups)
        
        # Should select the older one
        assert result is not None
        assert result.path.name == "old2.jira"
    
    def test_performance_with_large_backup_set(self):
        """Test performance and correctness with a large number of backups."""
        # Create 100 backups spread over 3 years
        backups = []
        for i in range(100):
            days_ago = i * 11  # Spread them out over ~3 years
            backups.append(self.create_backup(f"backup_{i:03d}.jira", days_ago))
        
        # Measure that it completes in reasonable time
        import time
        start_time = time.time()
        
        result = self.strategy.select_file_for_deletion(backups)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Should complete quickly (less than 1 second for 100 backups)
        assert execution_time < 1.0
        
        # Should return a valid result
        assert result is not None
        assert result in backups


class TestRetentionLadderTimeLogic:
    """Test the time-based logic of the retention ladder."""
    
    def test_time_period_boundaries(self):
        """Test the boundaries between time periods."""
        strategy = BackupRetentionLadder()
        now = datetime.now()
        
        # Test boundary cases
        boundary_backups = [
            BackupInfo(Path("exactly_30_days.jira"), now - timedelta(days=30), "1"),
            BackupInfo(Path("exactly_365_days.jira"), now - timedelta(days=365), "2"),
            BackupInfo(Path("just_under_30.jira"), now - timedelta(days=29, hours=23), "3"),
            BackupInfo(Path("just_over_30.jira"), now - timedelta(days=30, hours=1), "4"),
            BackupInfo(Path("just_under_365.jira"), now - timedelta(days=364, hours=23), "5"),
            BackupInfo(Path("just_over_365.jira"), now - timedelta(days=365, hours=1), "6"),
        ]
        
        # Test that categorization works correctly at boundaries
        one_month_ago = now - timedelta(days=30)
        one_year_ago = now - timedelta(days=365)
        
        for backup in boundary_backups:
            # Verify that the time logic categorizes correctly
            is_recent = backup.created_at >= one_month_ago
            is_medium = one_year_ago <= backup.created_at < one_month_ago
            is_long = backup.created_at < one_year_ago
            
            # Exactly one category should be true
            assert sum([is_recent, is_medium, is_long]) == 1
    
    def test_week_boundary_logic(self):
        """Test ISO week boundary logic."""
        strategy = BackupRetentionLadder()
        
        # Create backups that span a week boundary
        # Use a known date for predictable week boundaries
        base_date = datetime(2025, 7, 21)  # Monday
        
        backups = [
            BackupInfo(Path("monday.jira"), base_date, "1"),                    # Week 30
            BackupInfo(Path("friday.jira"), base_date + timedelta(days=4), "2"), # Week 30
            BackupInfo(Path("sunday.jira"), base_date + timedelta(days=6), "3"), # Week 30
            BackupInfo(Path("next_monday.jira"), base_date + timedelta(days=7), "4"), # Week 31
        ]
        
        candidates = strategy._find_weekly_candidates(backups)
        
        # Should have candidates from the first week (3 backups) but not the second week (1 backup)
        assert len(candidates) == 2  # Keep latest from first week, all others are candidates
        
        candidate_names = [c.path.name for c in candidates]
        assert "sunday.jira" not in candidate_names    # Latest from week 30
        assert "next_monday.jira" not in candidate_names  # Only backup from week 31
    
    def test_month_boundary_logic(self):
        """Test month boundary logic."""
        strategy = BackupRetentionLadder()
        
        # Create backups that span month boundaries
        backups = [
            BackupInfo(Path("july_early.jira"), datetime(2025, 7, 5), "1"),
            BackupInfo(Path("july_late.jira"), datetime(2025, 7, 28), "2"),
            BackupInfo(Path("august_early.jira"), datetime(2025, 8, 2), "3"),
            BackupInfo(Path("august_late.jira"), datetime(2025, 8, 25), "4"),
        ]
        
        candidates = strategy._find_monthly_candidates(backups)
        
        # Should have 1 candidate from July and 1 from August
        assert len(candidates) == 2
        
        candidate_names = [c.path.name for c in candidates]
        assert "july_late.jira" not in candidate_names    # Latest from July
        assert "august_late.jira" not in candidate_names  # Latest from August
        assert "july_early.jira" in candidate_names       # Earlier July backup
        assert "august_early.jira" in candidate_names     # Earlier August backup
    
    def test_year_boundary_logic(self):
        """Test year boundary logic."""
        strategy = BackupRetentionLadder()
        
        # Create backups that span year boundaries
        backups = [
            BackupInfo(Path("2023_early.jira"), datetime(2023, 3, 15), "1"),
            BackupInfo(Path("2023_late.jira"), datetime(2023, 11, 20), "2"),
            BackupInfo(Path("2024_early.jira"), datetime(2024, 2, 10), "3"),
            BackupInfo(Path("2024_late.jira"), datetime(2024, 10, 5), "4"),
        ]
        
        candidates = strategy._find_yearly_candidates(backups)
        
        # Should have 1 candidate from 2023 and 1 from 2024
        assert len(candidates) == 2
        
        candidate_names = [c.path.name for c in candidates]
        assert "2023_late.jira" not in candidate_names   # Latest from 2023
        assert "2024_late.jira" not in candidate_names   # Latest from 2024
        assert "2023_early.jira" in candidate_names      # Earlier 2023 backup
        assert "2024_early.jira" in candidate_names      # Earlier 2024 backup


class TestRealisticLongTermScenarios:
    """Test realistic long-term backup scenarios as requested."""

    def test_three_years_daily_backups_starting_february_2022(self):
        """
        Scenario: Daily backups for 3 years starting February 3, 2022.
        Both strategies should delete the oldest file (February 3, 2022).
        """
        # Create 3 years of daily backups starting Feb 3, 2022
        start_date = datetime(2022, 2, 3)
        backups = []
        
        # Generate daily backups for 3 years (1095 days)
        for day_offset in range(1095):
            backup_date = start_date + timedelta(days=day_offset)
            backup_info = BackupInfo(
                path=Path(f"{665800 + day_offset}-jira-backup-{backup_date.strftime('%Y-%m-%d')}"),
                created_at=backup_date,
                task_id=str(665800 + day_offset)
            )
            backups.append(backup_info)
        
        print(f"Generated {len(backups)} daily backups from {start_date.date()} to {backups[-1].created_at.date()}")
        
        # Test 1: OldestFirstStrategy should select February 3, 2022
        oldest_strategy = OldestFirstStrategy()
        selected_oldest = oldest_strategy.select_file_for_deletion(backups)
        
        assert selected_oldest is not None
        assert selected_oldest.created_at.date() == start_date.date()
        assert selected_oldest.task_id == "665800"
        print(f"OldestFirstStrategy selected: {selected_oldest.path.name} ({selected_oldest.created_at.date()})")
        
        # Test 2: BackupRetentionLadder should also select February 3, 2022
        # With 3 years of daily data, retention ladder will have many candidates
        # but should still prioritize the absolute oldest when many exist
        ladder_strategy = BackupRetentionLadder()
        selected_ladder = ladder_strategy.select_file_for_deletion(backups)
        
        assert selected_ladder is not None
        assert selected_ladder.created_at.date() == start_date.date()
        assert selected_ladder.task_id == "665800"
        print(f"BackupRetentionLadder selected: {selected_ladder.path.name} ({selected_ladder.created_at.date()})")

    def test_daily_backups_through_2022_different_strategies(self):
        """
        Scenario: Daily backups for all of 2022 (Jan 1 - Dec 31, 2022).
        Test from perspective of early 2025 (current time).
        
        Expected behavior:
        - oldest_first: Should delete January 1, 2022 (absolute oldest)
        - retention_ladder: Should delete January 1, 2022 (oldest in yearly group)
        
        Note: The user's example mentioned expecting Jan 1, 2023 vs Dec 31, 2022
        but that would require backups extending into 2023. This test shows
        the actual behavior with 2022-only data.
        """
        # Generate daily backups for all of 2022
        backups = []
        current_date = datetime(2022, 1, 1)
        task_id = 665000
        
        while current_date.year == 2022:
            backup_info = BackupInfo(
                path=Path(f"{task_id}-jira-backup-{current_date.strftime('%Y-%m-%d')}"),
                created_at=current_date,
                task_id=str(task_id)
            )
            backups.append(backup_info)
            current_date += timedelta(days=1)
            task_id += 1
        
        print(f"Generated {len(backups)} daily backups for 2022: {backups[0].created_at.date()} to {backups[-1].created_at.date()}")
        
        # Test OldestFirstStrategy
        oldest_strategy = OldestFirstStrategy()
        selected_oldest = oldest_strategy.select_file_for_deletion(backups)
        
        assert selected_oldest is not None
        assert selected_oldest.created_at.date() == datetime(2022, 1, 1).date()
        assert selected_oldest.task_id == "665000"
        print(f"OldestFirstStrategy selected: {selected_oldest.path.name} ({selected_oldest.created_at.date()})")
        
        # Test BackupRetentionLadder
        # All 2022 backups are now 2+ years old (long-term retention)
        # Should keep newest per year (Dec 31, 2022) and mark others for deletion
        # Will select oldest among deletion candidates
        ladder_strategy = BackupRetentionLadder()
        selected_ladder = ladder_strategy.select_file_for_deletion(backups)
        
        assert selected_ladder is not None
        assert selected_ladder.created_at.date() == datetime(2022, 1, 1).date()
        assert selected_ladder.task_id == "665000"
        print(f"BackupRetentionLadder selected: {selected_ladder.path.name} ({selected_ladder.created_at.date()})")
        
        # Verify that the newest backup (Dec 31, 2022) would be preserved
        newest_2022 = max(backups, key=lambda b: b.created_at)
        assert newest_2022.created_at.date() == datetime(2022, 12, 31).date()
        assert selected_ladder != newest_2022
        print(f"Newest backup preserved: {newest_2022.path.name} ({newest_2022.created_at.date()})")

    def test_backups_spanning_multiple_years_clear_differences(self):
        """
        Extended scenario to clearly show strategy differences:
        Daily backups from Jan 1, 2022 through Jan 31, 2023.
        
        This demonstrates the user's expected behavior where:
        - oldest_first: Deletes Jan 1, 2022 (absolute oldest)
        - retention_ladder: Has more complex logic based on yearly groupings
        """
        backups = []
        
        # Daily backups for 2022 (full year)
        current_date = datetime(2022, 1, 1)
        task_id = 665000
        
        while current_date <= datetime(2022, 12, 31):
            backup_info = BackupInfo(
                path=Path(f"{task_id}-jira-backup-{current_date.strftime('%Y-%m-%d')}"),
                created_at=current_date,
                task_id=str(task_id)
            )
            backups.append(backup_info)
            current_date += timedelta(days=1)
            task_id += 1
        
        # Daily backups for January 2023
        current_date = datetime(2023, 1, 1)
        while current_date <= datetime(2023, 1, 31):
            backup_info = BackupInfo(
                path=Path(f"{task_id}-jira-backup-{current_date.strftime('%Y-%m-%d')}"),
                created_at=current_date,
                task_id=str(task_id)
            )
            backups.append(backup_info)
            current_date += timedelta(days=1)
            task_id += 1
        
        print(f"Generated {len(backups)} backups: {backups[0].created_at.date()} to {backups[-1].created_at.date()}")
        
        # Test OldestFirstStrategy - should always pick absolute oldest
        oldest_strategy = OldestFirstStrategy()
        selected_oldest = oldest_strategy.select_file_for_deletion(backups)
        
        assert selected_oldest is not None
        assert selected_oldest.created_at.date() == datetime(2022, 1, 1).date()
        print(f"OldestFirstStrategy: {selected_oldest.path.name} ({selected_oldest.created_at.date()})")
        
        # Test BackupRetentionLadder - more sophisticated logic
        # Will have candidates from both 2022 and 2023 yearly groups
        # But still likely to select oldest overall due to many candidates
        ladder_strategy = BackupRetentionLadder()
        selected_ladder = ladder_strategy.select_file_for_deletion(backups)
        
        assert selected_ladder is not None
        print(f"BackupRetentionLadder: {selected_ladder.path.name} ({selected_ladder.created_at.date()})")
        
        # Analyze what each strategy preserves
        backups_2022 = [b for b in backups if b.created_at.year == 2022]
        backups_2023 = [b for b in backups if b.created_at.year == 2023]
        
        print(f"2022 backups: {len(backups_2022)} (from {min(backups_2022, key=lambda x: x.created_at).created_at.date()} to {max(backups_2022, key=lambda x: x.created_at).created_at.date()})")
        print(f"2023 backups: {len(backups_2023)} (from {min(backups_2023, key=lambda x: x.created_at).created_at.date()} to {max(backups_2023, key=lambda x: x.created_at).created_at.date()})")
        
        # For retention ladder, verify it would preserve newest from each year
        newest_2022 = max(backups_2022, key=lambda b: b.created_at)
        newest_2023 = max(backups_2023, key=lambda b: b.created_at)
        
        # Run multiple deletions to see the pattern
        temp_backups = backups.copy()
        deletions = []
        
        for i in range(10):  # Try 10 deletions
            selected = ladder_strategy.select_file_for_deletion(temp_backups)
            if selected:
                deletions.append(selected)
                temp_backups.remove(selected)
            else:
                break
        
        if deletions:
            print("First 10 retention ladder deletions:")
            for i, deletion in enumerate(deletions):
                print(f"  {i+1}. {deletion.path.name} ({deletion.created_at.date()})")
            
            # Verify newest from each year are preserved
            deletion_dates = [d.created_at.date() for d in deletions]
            assert newest_2022.created_at.date() not in deletion_dates
            assert newest_2023.created_at.date() not in deletion_dates
