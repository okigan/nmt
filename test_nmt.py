from nmt import *
from smart import smart_exists, smart_delete

bbb_source = "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4"
bbb_detination = "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/test_output/out.mp4"


def test_echo():
    assert echo(12) == 12

def test_main():
    assert main() != None


def test_analyze():
    frame_data = analyze_source(bbb_source)

    assert len(frame_data.frames) > 0


def test_concatenate_to_local():
    http_source = Source(bbb_source)
    http_source.inpoint = 0
    http_source.outpoint = 1
    sources = [http_source] * 2
    dest = tempfile.mktemp(util.get_curr_function_name())

    assert not os.path.exists(dest)
    concatenate(sources, dest)
    assert os.path.exists(dest)
    os.remove(dest)


def test_concatenate_to_s3():
    http_source = Source(bbb_source)
    http_source.inpoint = 0
    http_source.outpoint = 1
    sources = [http_source] * 2
    dest = bbb_detination

    assert not os.path.exists(dest)
    concatenate(sources, bbb_detination)
    assert smart_exists(dest)


def test_httpfy_cloud():
    s = "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4"

    h = httpfy_cloud(s)

    assert len(h) > 1


def test_smart_exits():
    assert smart_exists(bbb_source) == True


def test_encode_chunk_to_s3():
    s = "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4"
    d = "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/test_output/bbb_sunflower_1080p_30fps_normal.mp4"


    smart_delete(d)
    assert not smart_exists(d)
    encode_chunk(d, s, 0, 1, 1, 1)

    assert smart_exists(d)
    smart_delete(d)


def test_transcode_local():
    s = "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4"
    d = "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/test_output/bbb_sunflower_1080p_30fps_normal.mp4"

    transcode(s, d, 0.1, 20.9)

    smart_delete(d)
