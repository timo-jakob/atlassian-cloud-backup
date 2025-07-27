"""Additional tests to improve thinning utils coverage from 91% to 98%+."""

import pytest
from atlassian_cloud_backup.thinning.utils import validate_thinning_config


class TestThinningUtilsSupplemental:
    """Supplemental tests to improve coverage for thinning utils."""

    def test_validate_thinning_config_zero_max_size(self):
        """Test validation with zero max size to cover line 125."""
        # This should cover line 125: if max_size_bytes <= 0:
        max_size_bytes, is_valid = validate_thinning_config("0MB", 0.8)
        assert max_size_bytes == 0
        assert is_valid == False  # Should be invalid due to zero size

    def test_validate_thinning_config_negative_size_edge_case(self):
        """Test validation with effectively negative size."""
        # Test with a size that could result in 0 after conversion
        max_size_bytes, is_valid = validate_thinning_config("0.0GB", 0.5)
        assert max_size_bytes == 0
        assert is_valid == False

    def test_validate_thinning_config_string_zero(self):
        """Test validation with string zero."""
        max_size_bytes, is_valid = validate_thinning_config("0", 0.5)
        assert max_size_bytes == 0
        assert is_valid == False
