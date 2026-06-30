import pytest

from pdman.range_response import (
    ContentRange,
    RangeResponseValidationError,
    parse_content_range,
    validate_range_response,
)


def test_parse_content_range_parses_known_total():
    assert parse_content_range("bytes 0-1023/4096") == ContentRange(
        start=0,
        end=1023,
        total=4096,
    )


def test_parse_content_range_parses_unknown_total():
    assert parse_content_range("bytes 0-1023/*") == ContentRange(
        start=0,
        end=1023,
        total=None,
    )


@pytest.mark.parametrize(
    "value",
    [
        "items 0-1023/4096",
        "bytes 1023-0/4096",
        "bytes 0-1023",
        "bytes a-b/4096",
        "bytes 0-4096/4096",
    ],
)
def test_parse_content_range_rejects_invalid_values(value):
    with pytest.raises(RangeResponseValidationError):
        parse_content_range(value)


def test_validate_range_response_accepts_matching_206():
    validate_range_response(
        status=206,
        requested_start=0,
        requested_end=1023,
        file_size=4096,
        content_range="bytes 0-1023/4096",
    )


def test_validate_range_response_accepts_matching_206_with_unknown_total():
    validate_range_response(
        status=206,
        requested_start=0,
        requested_end=1023,
        file_size=4096,
        content_range="bytes 0-1023/*",
    )


@pytest.mark.parametrize(
    "kwargs, message",
    [
        (
            {
                "status": 206,
                "requested_start": 0,
                "requested_end": 1023,
                "file_size": 4096,
                "content_range": None,
            },
            "missing Content-Range",
        ),
        (
            {
                "status": 206,
                "requested_start": 1024,
                "requested_end": 2047,
                "file_size": 4096,
                "content_range": "bytes 0-1023/4096",
            },
            "start mismatch",
        ),
        (
            {
                "status": 206,
                "requested_start": 1024,
                "requested_end": 2047,
                "file_size": 4096,
                "content_range": "bytes 1024-4095/4096",
            },
            "end mismatch",
        ),
        (
            {
                "status": 206,
                "requested_start": 1024,
                "requested_end": 2047,
                "file_size": 4096,
                "content_range": "bytes 1024-2047/8192",
            },
            "total mismatch",
        ),
        (
            {
                "status": 200,
                "requested_start": 1024,
                "requested_end": 2047,
                "file_size": 4096,
                "content_range": None,
            },
            "only valid for full-file range",
        ),
        (
            {
                "status": 503,
                "requested_start": 1024,
                "requested_end": 2047,
                "file_size": 4096,
                "content_range": None,
            },
            "Unexpected range response status",
        ),
    ],
)
def test_validate_range_response_rejects_bad_responses(kwargs, message):
    with pytest.raises(RangeResponseValidationError, match=message):
        validate_range_response(**kwargs)


def test_validate_range_response_accepts_200_for_full_file_range():
    validate_range_response(
        status=200,
        requested_start=0,
        requested_end=4095,
        file_size=4096,
        content_range=None,
    )
