#!/usr/bin/env python3
"""
Test script to verify the smart field detection functionality
"""

import re
from app.processor import FIELD_DETECTION_PATTERNS, is_acroform_field_already_filled

def test_field_detection():
    """Test the field detection patterns"""
    
    test_cases = [
        # Email tests
        ("email", "test@example.com", True),
        ("email", "user@domain.org", True),
        ("email", "john.doe@company.co.uk", True),
        ("email", "invalid-email", False),
        ("email", "", False),
        ("email", "   ", False),
        
        # Phone tests
        ("phone", "(555) 123-4567", True),
        ("phone", "555-123-4567", True),
        ("phone", "555.123.4567", True),
        ("phone", "5551234567", True),
        ("phone", "123", False),  # Too few digits
        ("phone", "abc", False),
        ("phone", "", False),
        
        # SSN tests
        ("ssn", "123-45-6789", True),
        ("ssn", "123456789", True),
        ("ssn", "12-34-5678", True),
        ("ssn", "123", False),
        ("ssn", "abc", False),
        
        # EIN tests
        ("ein", "12-3456789", True),
        ("ein", "123456789", True),
        ("ein", "12-345678", False),
        ("ein", "abc", False),
        
        # DOB tests
        ("dob", "01/15/1990", True),
        ("dob", "1/5/90", True),
        ("dob", "01-15-1990", True),
        ("dob", "1990/01/15", False),  # Wrong format
        ("dob", "abc", False),
        
        # Name tests
        ("name", "John Doe", True),
        ("name", "A", False),  # Too short
        ("name", "123", False),
        ("name", "", False),
        
        # Address tests
        ("address", "123 Main Street, Springfield, IL 62701", True),
        ("address", "Short", False),  # Too short
        ("address", "123", False),
        ("address", "", False),
    ]
    
    print("Testing field detection patterns...")
    print("=" * 50)
    
    passed = 0
    total = len(test_cases)
    
    for field_type, value, expected in test_cases:
        # Test the pattern matching
        if field_type in FIELD_DETECTION_PATTERNS:
            pattern = FIELD_DETECTION_PATTERNS[field_type]
            match = re.search(pattern, value, re.IGNORECASE)
            pattern_result = bool(match)
        else:
            pattern_result = False
        
        # Test the function
        function_result = is_acroform_field_already_filled(field_type, value)
        
        # Determine if test passed
        test_passed = (pattern_result == expected) and (function_result == expected)
        
        status = "✓ PASS" if test_passed else "✗ FAIL"
        print(f"{status} {field_type:8} | '{value:20}' | Expected: {expected} | Pattern: {pattern_result} | Function: {function_result}")
        
        if test_passed:
            passed += 1
    
    print("=" * 50)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Field detection is working correctly.")
    else:
        print("❌ Some tests failed. Please review the field detection logic.")
    
    return passed == total

if __name__ == "__main__":
    test_field_detection()
