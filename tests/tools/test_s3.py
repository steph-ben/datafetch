from datafetch.tools.s3 import S3ApiBucket


def test_generic():
    s3api = S3ApiBucket(bucket_name="any_bucket")
    assert isinstance(s3api, S3ApiBucket)
    r = s3api.filter(Prefix="plop")
    assert r is not None
