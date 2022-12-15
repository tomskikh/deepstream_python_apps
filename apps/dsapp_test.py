#!/usr/bin/env python3
import sys
import time

sys.path.append('../../')

import numpy as np
import cv2
import gi

gi.require_version('Gst', '1.0')
from gi.repository import GLib, Gst
from common.is_aarch_64 import is_aarch64
from common.bus_call import bus_call
import pyds


def pad_buffer_probe(pad: Gst.Pad, info: Gst.PadProbeInfo, unmap: bool):
    gst_buffer: Gst.Buffer = info.get_buffer()
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    l_frame = batch_meta.frame_meta_list
    while l_frame is not None:
        try:
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        except StopIteration:
            break

        n_frame: np.ndarray = pyds.get_nvds_buf_surface(hash(gst_buffer), frame_meta.batch_id)

        cv2.rectangle(n_frame, (150, 50), (600, 300), (0, 255, 0, 0), 4)
        frame_bytes = n_frame.tobytes()
        print(
            f'{pad.get_parent().get_name()} | #{frame_meta.frame_num}'
            f' resolution: {n_frame.shape}'
            f' size: {len(frame_bytes)} bytes'
        )

        if unmap:
            pyds.unmap_nvds_buf_surface(hash(gst_buffer), frame_meta.batch_id)

        try:
            l_frame = l_frame.next
        except StopIteration:
            break

    return Gst.PadProbeReturn.OK


def cb_newpad(decodebin, decoder_src_pad, source_id):
    caps = decoder_src_pad.get_current_caps()
    gststruct = caps.get_structure(0)
    gstname = gststruct.get_name()

    if gstname.find("video") != -1:
        pad_name = f"sink_{source_id}"
        pipeline = decodebin.get_parent()

        # Convert to "video/x-raw(memory:NVMM), format=RGBA"
        # to be free to use pyds.get_nvds_buf_surface after nvstreammux.
        nvvidconv = Gst.ElementFactory.make("nvvideoconvert")
        if not is_aarch64():
            nvvidconv.set_property("nvbuf-memory-type", int(pyds.NVBUF_MEM_CUDA_UNIFIED))
        pipeline.add(nvvidconv)
        nvvidconv.sync_state_with_parent()
        caps = Gst.Caps.from_string("video/x-raw(memory:NVMM), format=RGBA")
        caps_filter = Gst.ElementFactory.make("capsfilter")
        caps_filter.set_property("caps", caps)
        pipeline.add(caps_filter)
        caps_filter.sync_state_with_parent()
        nvvidconv.link(caps_filter)
        nvvidconv_sink_pad = nvvidconv.get_static_pad('sink')
        assert decoder_src_pad.link(nvvidconv_sink_pad) == Gst.PadLinkReturn.OK
        src_pad = caps_filter.get_static_pad("src")

        streammux = pipeline.get_by_name('streammux')
        streammux_sink_pad = streammux.get_request_pad(pad_name)
        if not streammux_sink_pad:
            sys.stderr.write("Unable to create streammux sink pad \n")
        if src_pad.link(streammux_sink_pad) == Gst.PadLinkReturn.OK:
            print("Linked to muxer")
        else:
            sys.stderr.write("Failed to link to muxer\n")


def create_source_bin(index, uri):
    bin_name = f"source-bin-{index:02d}"
    print("Creating source bin", bin_name)
    nbin = Gst.ElementFactory.make("uridecodebin", bin_name)
    if not nbin:
        sys.stderr.write("Unable to create uri decode bin")
    nbin.set_property("uri", uri)
    nbin.connect("pad-added", cb_newpad, index)
    return nbin


def main(args):
    if len(args) < 1:
        sys.stderr.write("usage: %s <uri1> [uri2] ... [uriN]\n" % args[0])
        sys.exit(1)

    number_sources = len(args) - 1

    Gst.init(None)

    print("Creating Pipeline")
    pipeline = Gst.Pipeline()
    is_live = False

    print("Creating streammux")
    streammux = Gst.ElementFactory.make("nvstreammux", "streammux")
    pipeline.add(streammux)

    for i in range(number_sources):
        print("Creating source bin", i)
        uri_name = args[i + 1]
        if uri_name.find("rtsp://") == 0:
            is_live = True
        source_bin = create_source_bin(i, uri_name)
        pipeline.add(source_bin)

    # some elements here (pgie, sgie)

    print("Creating workload")
    workload_1 = Gst.ElementFactory.make("identity", "workload-1")
    pipeline.add(workload_1)
    workload_2 = Gst.ElementFactory.make("identity", "workload-2")
    pipeline.add(workload_2)
    workload_3 = Gst.ElementFactory.make("identity", "workload-3")
    pipeline.add(workload_3)
    workload_4 = Gst.ElementFactory.make("identity", "workload-4")
    pipeline.add(workload_4)

    print("Creating streamdemux")
    streamdemux = Gst.ElementFactory.make("nvstreamdemux", "streamdemux")
    pipeline.add(streamdemux)

    print("Creating queue")
    queue = Gst.ElementFactory.make("queue", "queue")
    pipeline.add(queue)

    # print("Creating converter")
    # converter = Gst.ElementFactory.make("nvvideoconvert", "converter")
    # pipeline.add(converter)
    #
    # print("Creating encoder")
    # encoder = Gst.ElementFactory.make("nvv4l2h264enc", "encoder")
    # pipeline.add(encoder)
    #
    # print("Creating parser")
    # parser = Gst.ElementFactory.make("h264parse", "parser")
    # pipeline.add(parser)

    print("Creating sink")
    # sink = Gst.ElementFactory.make("filesink", "sink")
    sink = Gst.ElementFactory.make("fakesink", "sink")
    pipeline.add(sink)

    if is_live:
        streammux.set_property('live-source', 1)
    streammux.set_property('width', 1280)
    streammux.set_property('height', 720)
    streammux.set_property('batch-size', number_sources)
    streammux.set_property('batched-push-timeout', 4000000)

    sink.set_property("sync", 0)
    sink.set_property("qos", 0)
    # sink.set_property("location", "/data/result.h264")

    if not is_aarch64():
        streammux.set_property("nvbuf-memory-type", int(pyds.NVBUF_MEM_CUDA_UNIFIED))
        # converter.set_property("nvbuf-memory-type", int(pyds.NVBUF_MEM_CUDA_UNIFIED))

    print("Linking elements in the Pipeline")

    assert streammux.link(workload_1)
    assert workload_1.link(workload_2)
    assert workload_2.link(streamdemux)

    streamdemux_src_pad = streamdemux.get_request_pad('src_0')
    # workload_3_sink_pad = workload_3.get_static_pad('sink')
    # assert streamdemux_src_pad.link(workload_3_sink_pad) == Gst.PadLinkReturn.OK
    queue_sink_pad = queue.get_static_pad('sink')
    assert streamdemux_src_pad.link(queue_sink_pad) == Gst.PadLinkReturn.OK

    assert queue.link(workload_3)
    assert workload_3.link(workload_4)
    assert workload_4.link(sink)
    # assert workload_4.link(converter)
    # assert converter.link(encoder)
    # assert encoder.link(parser)
    # assert parser.link(sink)
    # assert streammux.link(sink)

    # create an event loop and feed gstreamer bus messages to it
    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    for workload, unmap in [
        (workload_1, False),
        (workload_2, False),
        (workload_3, False),
        (workload_4, True),
    ]:
        sink_pad = workload.get_static_pad("sink")
        # sink_pad = sink.get_static_pad("sink")
        if not sink_pad:
            sys.stderr.write("Unable to get sink pad")
        else:
            sink_pad.add_probe(Gst.PadProbeType.BUFFER, pad_buffer_probe, unmap)

    print("Now playing...")
    for i, source in enumerate(args[:-1]):
        if i != 0:
            print(i, ": ", source)

    print("Starting pipeline")
    pipeline.set_state(Gst.State.PLAYING)
    try:
        loop.run()
    except:
        pass
    print("Exiting app\n")
    pipeline.set_state(Gst.State.NULL)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
