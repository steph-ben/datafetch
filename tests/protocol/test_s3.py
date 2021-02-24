from datafetch.protocol.s3 import S3ApiBucket


def test_s3_generic():
    s3api = S3ApiBucket(bucket_name="any_bucket")
    assert isinstance(s3api, S3ApiBucket)
    r = s3api.filter(Prefix="plop")
    assert r is not None


def test_s3_fetch(tmp_path):
    s3api = S3ApiBucket(bucket_name="any_bucket")
    r = s3api.fetch(object_key="plop", destination_dir=str(tmp_path))
    assert r is None
