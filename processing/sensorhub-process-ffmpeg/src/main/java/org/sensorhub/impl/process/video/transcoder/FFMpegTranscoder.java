/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.process.video.transcoder;

import net.opengis.swe.v20.*;
import org.bytedeco.ffmpeg.avcodec.AVPacket;
import org.bytedeco.ffmpeg.avutil.AVFrame;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.DoublePointer;
import org.bytedeco.javacpp.Pointer;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.processing.OSHProcessInfo;
import org.sensorhub.impl.process.video.transcoder.coders.Coder;
import org.sensorhub.impl.process.video.transcoder.coders.Decoder;
import org.sensorhub.impl.process.video.transcoder.coders.Encoder;
import org.sensorhub.impl.process.video.transcoder.coders.SwScaler;
import org.sensorhub.impl.process.video.transcoder.formatters.AVByteFormatter;
import org.sensorhub.impl.process.video.transcoder.formatters.PacketFormatter;
import org.sensorhub.impl.process.video.transcoder.formatters.RgbFormatter;
import org.sensorhub.impl.process.video.transcoder.formatters.YuvFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.data.DataBlockCompressed;
import org.vast.process.ExecutableProcessImpl;
import org.vast.process.ProcessException;
import org.vast.swe.SWEHelper;
import org.vast.swe.helper.RasterHelper;

import org.jspecify.annotations.Nullable;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.bytedeco.ffmpeg.global.avcodec.*;
import static org.bytedeco.ffmpeg.global.avutil.*;
import static org.bytedeco.ffmpeg.global.swscale.*;

/**
 * <p>
 * Implementation of video transcoder based on FFMPEG
 * </p>
 *
 * @author Kyle Fitzpatrick
 * @since Aug 2025
 */

public class FFMpegTranscoder extends ExecutableProcessImpl
{
    private static final Logger logger = LoggerFactory.getLogger(FFMpegTranscoder.class);
    private static final long PIPELINE_JOIN_TIMEOUT_MILLIS = 10_000L;
    private static final long OUTPUT_WAIT_MILLIS = 100L;

	public static final OSHProcessInfo INFO = new OSHProcessInfo("video:FFMpegTranscoder", "FFMPEG Video Transcoder", null, FFMpegTranscoder.class);

    AtomicBoolean doRun = new AtomicBoolean(true);
    AtomicBoolean isRunning = new AtomicBoolean(false);
    Time inputTimeStamp;
    Count inputWidth, inputHeight;
    DataArray imgIn;
    Time outputTimeStamp;
    Count outputWidth, outputHeight, inputFps, inputBitrate;
    DataArray imgOut;
    Text inCodecParam;
    Text outCodecParam;

    List<Coder<?, ?>> videoProcs;
    AVByteFormatter<Pointer> inputFormatter, outputFormatter;
    BlockingQueue<Pointer> inputPackets;
    BlockingQueue<Pointer> outputPackets;
    Thread outputThread;
    private Coder<?, ?> finalCoder;
    private final Object pipelineStateLock = new Object();
    private final AtomicReference<Throwable> pipelineFailure = new AtomicReference<>();
    private boolean acceptingInput;
    private boolean stopInProgress;
    private boolean processStopRequested;
    private AtomicBoolean outputRun = new AtomicBoolean(false);

    HashMap<String, Integer> decOptions = new HashMap<>();
    HashMap<String, Integer> encOptions = new HashMap<>();

    final boolean publish = false;
    CodecEnum inCodec;
    CodecEnum outCodec;
    RasterHelper swe = new RasterHelper();

    int width, height, outWidth, outHeight;

    public FFMpegTranscoder()
    {
    	super(INFO);
        RasterHelper swe = new RasterHelper();

        // inputs
        inputData.add("codedFrame", swe.createRecord()
                .name("inFrame")
                .label("Input Video Frame")
                .description("")
                .definition(SWEHelper.getPropertyUri("VideoFrame"))
                .addField("time", inputTimeStamp = swe.createTime()
                        .asSamplingTimeIsoUTC()
                        .build())
                .addField("width", inputWidth = swe.createCount()
                        .id("width")
                        .label("Input Frame Width")
                        .build())
                .addField("height", inputHeight = swe.createCount()
                        .id("height")
                        .label("Input Frame Height")
                        .build())
                .addField("sampleTime", inputTimeStamp = swe.createTime()
                        .asSamplingTimeIsoUTC()
                        .label("Sample Time")
                        .description("Time of data collection").build())
                .addField("img", imgIn = swe.newRgbImage(inputWidth, inputHeight, DataType.BYTE))
                .build());

        // parameters
        paramData.add("inCodec", inCodecParam = swe.createText()
                .definition(SWEHelper.getPropertyUri("Codec"))
                .label("Input Codec Name")
                .addAllowedValues(CodecEnum.class)
                .build());

        paramData.add("outCodec", outCodecParam = swe.createText()
                .definition(SWEHelper.getPropertyUri("Codec"))
                .label("Output Codec Name")
                .addAllowedValues(CodecEnum.class)
                .build());

        // outputs
        // Input image will be scaled to match output size
        outputData.add("outFrame", swe.createRecord()
                .name("outFrame")
                .label("Output Video Frame")
                .description("")
                .definition(SWEHelper.getPropertyUri("VideoFrame"))
                .addField("sampleTime", outputTimeStamp =  swe.createTime()
                        .asSamplingTimeIsoUTC()
                        .label("Sample Time")
                        .description("Time of data collection").build())
                .addField("outWidth", outputWidth = swe.createCount()
                        .id("outWidth")
                        .label("Output Frame Width")
                        .build())
                .addField("outHeight", outputHeight = swe.createCount()
                        .id("outHeight")
                        .label("Output Frame Height")
                        .build())
                .addField("img", imgOut = swe.newRgbImage(outputWidth, outputHeight, DataType.BYTE))
                .build());


    }

    /**
     * Initializes all encoder/decoder/swscaler objects. These objects are added to the {@link FFMpegTranscoder#videoProcs}
     * queue in the order they should process the incoming data. At most, the flow will be Decoder -> SWScale -> Encoder.
     */
    void initCoders() {
        if (isRunning.get()) {
            try {
                stopProcessThreads();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while reinitializing transcoder", e);
            }
        }

        List<Coder<?, ?>> coders = new ArrayList<>();

        if (!isUncompressed(inCodec)) {
            coders.add(new Decoder(inCodec.ffmpegId, decOptions));
        }
        int inFmt = isUncompressed(inCodec) ? inCodec.ffmpegId : AV_PIX_FMT_YUV420P;
        int outFmt = isUncompressed(outCodec) ? outCodec.ffmpegId : AV_PIX_FMT_YUV420P;
        if (width != outWidth || height != outHeight || inFmt != outFmt
                || (isUncompressed(inCodec) && isUncompressed(outCodec))) {
            coders.add(new SwScaler(inFmt, outFmt, width, height, outWidth, outHeight));
        }
        if (!isUncompressed(outCodec)) {
            coders.add(new Encoder(outCodec.ffmpegId, encOptions));
        }

        wireCoders(coders);
    }

    /**
     * Connects codec stages with blocking ownership-transfer queues. Kept separate from codec
     * selection so the same wiring and lifecycle can be exercised without native codecs in tests.
     */
    void wireCoders(List<Coder<?, ?>> coders) {
        if (coders == null || coders.isEmpty()) {
            throw new IllegalStateException("Transcoder requires at least one codec or scaler stage");
        }
        synchronized (pipelineStateLock) {
            if (isRunning.get() || hasLivePipelineThreads()) {
                throw new IllegalStateException(
                        "Cannot replace a transcoder pipeline while its generation is active");
            }
        }

        videoProcs = new ArrayList<>(coders);
        inputPackets = new LinkedBlockingQueue<>();
        videoProcs.get(0).setInQueue(inputPackets);
        for (int i = 1; i < videoProcs.size(); i++) {
            videoProcs.get(i).setInQueue(videoProcs.get(i - 1).getOutQueue());
        }

        finalCoder = videoProcs.get(videoProcs.size() - 1);
        outputPackets = asPointerQueue(finalCoder.getOutQueue());
        for (Coder<?, ?> coder : videoProcs) {
            coder.setFailureHandler(this::recordPipelineFailure);
        }
        pipelineFailure.set(null);
        outputThread = null;
    }

    /**
     * Invoked during the first call to {@link FFMpegTranscoder#execute()} (when {@link FFMpegTranscoder#isRunning} is false).
     * Start all {@link Coder} thread objects stored in {@link FFMpegTranscoder#videoProcs}.
     */
    void startProcessThreads() {
        synchronized (pipelineStateLock) {
            if (processStopRequested) {
                throw new IllegalStateException(
                        "Transcoder process is stopped; call start() before executing again");
            }
            if (isRunning.get()) {
                if (acceptingInput && !stopInProgress) {
                    return;
                }
                throw new IllegalStateException(
                        "Previous transcoder pipeline has not terminated; restart is blocked");
            }
            if (hasLivePipelineThreads()) {
                isRunning.set(true);
                acceptingInput = false;
                throw new IllegalStateException(
                        "Previous transcoder threads are still alive; restart is blocked");
            }
            if (videoProcs == null || videoProcs.isEmpty()
                    || videoProcs.get(0).getState() != Thread.State.NEW) {
                initCoders();
            }

            doRun.set(true);
            pipelineFailure.set(null);
            outputRun = new AtomicBoolean(true);
            BlockingQueue<Pointer> generationOutput = outputPackets;
            Coder<?, ?> generationFinalCoder = finalCoder;
            List<Coder<?, ?>> generationCoders = List.copyOf(videoProcs);
            AtomicBoolean generationRun = outputRun;
            outputThread = new Thread(
                    () -> outputProcess(generationOutput, generationFinalCoder,
                            generationCoders, generationRun),
                    "ffmpeg-transcoder-output");
            isRunning.set(true);
            acceptingInput = true;
            try {
                outputThread.start();
                for (Coder<?, ?> process : videoProcs) {
                    process.start();
                }
            } catch (Throwable t) {
                recordPipelineFailure(t);
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                }
                if (t instanceof Error) {
                    throw (Error)t;
                }
                throw new IllegalStateException("Could not start transcoder pipeline", t);
            }
        }
    }

    /**
     * Invoked on process stop and init.
     * Stop all {@link Coder} thread objects stored in {@link FFMpegTranscoder#videoProcs}.
     */
    void stopProcessThreads() throws InterruptedException {
        List<Coder<?, ?>> generationCoders;
        Thread generationOutputThread;
        BlockingQueue<Pointer> generationInput;
        BlockingQueue<Pointer> generationOutput;
        Coder<?, ?> generationFinalCoder;
        AtomicBoolean generationRun;

        synchronized (pipelineStateLock) {
            while (stopInProgress) {
                pipelineStateLock.wait();
            }
            if (!isRunning.get()) {
                if (hasLivePipelineThreads()) {
                    isRunning.set(true);
                    throw new IllegalStateException(
                            "Transcoder threads are still alive; restart remains blocked");
                }
                return;
            }
            // Close input before any worker observes end-of-input. execute() takes this same lock,
            // so a frame is either queued before shutdown starts (and drained) or rejected.
            acceptingInput = false;
            stopInProgress = true;
            generationCoders = List.copyOf(videoProcs);
            generationOutputThread = outputThread;
            generationInput = inputPackets;
            generationOutput = outputPackets;
            generationFinalCoder = finalCoder;
            generationRun = outputRun;
        }

        Throwable failure = pipelineFailure.get();
        boolean stopTimedOut = false;
        boolean fullyTerminated = false;
        long joinTimeoutMillis = pipelineJoinTimeoutMillis();
        try {
            if (failure == null) {
                // Each stage must finish only after its upstream producer has stopped. This
                // preserves delayed output and prevents downstream end-of-stream from racing an
                // upstream flush.
                for (Coder<?, ?> coder : generationCoders) {
                    coder.finishInput();
                    if (!joinWithin(coder, joinTimeoutMillis)) {
                        stopTimedOut = true;
                        failure = new IllegalStateException(
                                coder.getClass().getSimpleName() + " did not finish within "
                                        + joinTimeoutMillis + " ms");
                        recordPipelineFailure(failure);
                        break;
                    }
                    if (coder.getFailure() != null) {
                        failure = coder.getFailure();
                        recordPipelineFailure(failure);
                        break;
                    }
                    if (!coder.isOutputComplete()) {
                        failure = new IllegalStateException(
                                coder.getClass().getSimpleName()
                                        + " stopped before completing output");
                        recordPipelineFailure(failure);
                        break;
                    }
                }
            }

            if (failure == null && generationOutputThread != null
                    && !joinWithin(generationOutputThread, joinTimeoutMillis)) {
                stopTimedOut = true;
                failure = new IllegalStateException(
                        "Transcoder output did not drain within " + joinTimeoutMillis + " ms");
                recordPipelineFailure(failure);
            }

            generationRun.set(false);
            doRun.set(false);
            if (failure != null) {
                cancelWorkers(generationCoders);
            }

            joinWorkersAfterCancellation(generationCoders, joinTimeoutMillis);
            if (generationOutputThread != null && generationOutputThread.isAlive()) {
                generationOutputThread.interrupt();
                generationOutputThread.join(joinTimeoutMillis);
            }

            fullyTerminated = allTerminated(generationCoders, generationOutputThread);
            if (!fullyTerminated) {
                IllegalStateException stuckFailure = new IllegalStateException(
                        "Transcoder threads did not terminate; restart is blocked",
                        failure);
                recordPipelineFailure(stuckFailure);
                throw stuckFailure;
            }

            drainUnconsumedQueues(generationInput, generationOutput, generationFinalCoder);
            synchronized (pipelineStateLock) {
                isRunning.set(false);
            }

            if (stopTimedOut) {
                throw new IllegalStateException(
                        "Transcoder shutdown timed out; all threads were eventually cancelled",
                        failure);
            }
        } catch (InterruptedException e) {
            generationRun.set(false);
            doRun.set(false);
            cancelWorkers(generationCoders);
            if (generationOutputThread != null) {
                generationOutputThread.interrupt();
            }
            throw e;
        } finally {
            synchronized (pipelineStateLock) {
                if (!fullyTerminated) {
                    // Keep the generation owned and non-restartable until a later stop verifies
                    // that every old thread is dead.
                    isRunning.set(true);
                    acceptingInput = false;
                }
                stopInProgress = false;
                pipelineStateLock.notifyAll();
            }
        }
    }

    protected long pipelineJoinTimeoutMillis() {
        return PIPELINE_JOIN_TIMEOUT_MILLIS;
    }


    @Override
    public void notifyParamChange()
    {
        boolean outputSizeChanged = false;

        // we need to set output frame dimensions if it was set to a fixed value
        if (inputWidth.getData() != null && inputWidth.getData().getIntValue() > 0)
        {
            if (!outputWidth.hasData())
                outputWidth.getParent().assignNewDataBlock();
            outputWidth.getData().setIntValue(inputWidth.getData().getIntValue());
            outputSizeChanged = true;
        }

        if (inputHeight.getData() != null && inputHeight.getData().getIntValue() > 0)
        {
            if (!outputHeight.hasData())
                outputHeight.getParent().assignNewDataBlock();
            outputHeight.getData().setIntValue(inputHeight.getData().getIntValue());
            outputSizeChanged = true;
        }

        if (outputSizeChanged == true)
        {
            /*((DataArray)imgOut.getElementType()).updateSize(outputWidth.getData().getIntValue());
            imgOut.updateSize(outputHeight.getData().getIntValue());*/
            ((DataArray)imgOut).updateSize();
        }
    }

    @Override
    public synchronized void stop() {
        // Prevent an execution already in flight from recreating the native pipeline after the
        // base executor is cancelled. The flag remains set until an explicit process start.
        synchronized (pipelineStateLock) {
            processStopRequested = true;
            acceptingInput = false;
        }
        // Quiesce the base execution loop before stopping its asynchronously owned codec stages.
        // This must happen even when native pipeline teardown later times out or fails.
        super.stop();

        if (isRunning.get()) {
            try {
                stopProcessThreads();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while stopping process threads");
                abortPipeline();
                throw new IllegalStateException("Interrupted while stopping transcoder pipeline", e);
            }
        }
    }

    @Override
    public synchronized void start(ExecutorService threadPool, Consumer<Throwable> onError)
            throws ProcessException {
        boolean restoreStoppedBoundary;
        synchronized (pipelineStateLock) {
            restoreStoppedBoundary = processStopRequested;
            processStopRequested = false;
        }
        try {
            super.start(threadPool, onError);
        } catch (ProcessException | RuntimeException | Error e) {
            // Executor submission can fail after the base class marks itself started. Restore both
            // halves of the stopped boundary so a failed explicit start cannot authorize a stale
            // execution left behind by the previous generation.
            super.stop();
            synchronized (pipelineStateLock) {
                processStopRequested = restoreStoppedBoundary;
                if (restoreStoppedBoundary) {
                    acceptingInput = false;
                }
            }
            throw e;
        }
    }

    /**
     * Add codecs for input/output video to the corresponding data structure encodings.
     * Helps identify codec used during serialization.
     */
    private void setImgEncoding() {
        var dataEncIn = swe.newBinaryEncoding(ByteOrder.BIG_ENDIAN, ByteEncoding.RAW);

        BinaryComponent timeEncIn = swe.newBinaryComponent();
        timeEncIn.setRef("/" + ((DataRecord)inputData.get(0)).getComponent(0).getName());
        timeEncIn.setCdmDataType(DataType.DOUBLE);
        dataEncIn.addMemberAsComponent(timeEncIn);

        var compressedBlockIn = swe.newBinaryBlock();
        compressedBlockIn.setRef("/" + ((DataRecord)inputData.get(0)).getComponent("img").getName());
        compressedBlockIn.setCompression(inCodecParam.getData().getStringValue().toUpperCase());
        dataEncIn.addMemberAsBlock(compressedBlockIn);
        try {
            SWEHelper.assignBinaryEncoding((DataRecord) inputData.get(0), dataEncIn);
        } catch (Exception e) {
            logger.warn("Invalid encoding");
        }
        imgIn.setEncoding(dataEncIn);

        var dataEncOut = swe.newBinaryEncoding(ByteOrder.BIG_ENDIAN, ByteEncoding.RAW);

        BinaryComponent timeEnc = swe.newBinaryComponent();
        timeEnc.setRef("/" + ((DataRecord)outputData.get(0)).getComponent(0).getName());
        timeEnc.setCdmDataType(DataType.DOUBLE);
        dataEncOut.addMemberAsComponent(timeEnc);

        var compressedBlock = swe.newBinaryBlock();
        compressedBlock.setRef("/" + ((DataRecord)outputData.get(0)).getComponent("img").getName());
        compressedBlock.setCompression(outCodecParam.getData().getStringValue().toUpperCase());
        dataEncOut.addMemberAsBlock(compressedBlock);
        try {
            SWEHelper.assignBinaryEncoding((DataRecord) outputData.get(0), dataEncOut);
        } catch (Exception e) {
            logger.warn("Invalid encoding");
        }
        imgOut.setEncoding(dataEncOut);
    }

    @Override
    public void init() throws ProcessException
    {
        if (isRunning.get()) {
            try {
                stopProcessThreads();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException("Interrupted while reinitializing transcoder", e);
            } catch (IllegalStateException e) {
                throw new ProcessException("Could not stop active transcoder during re-init", e);
            }
        }
        synchronized (pipelineStateLock) {
            if (hasLivePipelineThreads()) {
                isRunning.set(true);
                acceptingInput = false;
                throw new ProcessException(
                        "Previous transcoder threads are still alive; re-init is blocked");
            }
            doRun.set(true);
        }

        // init decoder according to configured codec
        // TODO: Automatically detect input codec from compression in data struct?
        try
        {
            inCodec = CodecEnum.valueOf(inCodecParam.getData().getStringValue().toUpperCase());
            outCodec = CodecEnum.valueOf(outCodecParam.getData().getStringValue().toUpperCase());

            setImgEncoding();

            initCodecOptions();

            // processThreads are always running, passing available data from decoder to encoder and encoder to output
            initCoders();

            inputFormatter = getFormatter(inCodec, width, height);
            outputFormatter = getFormatter(outCodec, outWidth, outHeight);

            imgOut.setData(new DataBlockCompressed());
        }
        catch (IllegalArgumentException e)
        {
            reportError("Unsupported codec" + inCodecParam.getData().getStringValue() + ". Must be one of " + Arrays.toString(CodecEnum.values()));
        }

        super.init();
    }

    /**
     * Creates maps of options for the encoder/decoder, including framerate, pixel format, bitrate, and image size.
     */
    private void initCodecOptions() {
        decOptions = new HashMap<>();
        encOptions = new HashMap<>();

        //DataComponent temp;
        int fps = safeGetCountVal(inputFps);
        if (fps > 0) {
            decOptions.put("fps", fps);
            encOptions.put("fps", fps);
        }

        encOptions.put("pix_fmt", AV_PIX_FMT_YUV420P);
        decOptions.put("pix_fmt", AV_PIX_FMT_YUV420P);

        int bitrate = safeGetCountVal(inputBitrate);
        if (bitrate > 0) {
            decOptions.put("bit_rate", bitrate);
            encOptions.put("bit_rate", bitrate); // Just assuming input br is the same as out, could this change?
        }

        width = safeGetCountVal(inputWidth);
        if (width <= 0) {
            width = imgIn.getComponent("row").getComponentCount();
        }
        decOptions.put("width", width);

        height = safeGetCountVal(inputHeight);
        if (height <= 0) {
            height = imgIn.getComponentCount();
        }
        decOptions.put("height", height);


        outHeight = safeGetCountVal(outputHeight);
        if (outHeight <= 0) {
            try {
                outHeight = imgOut.getComponentCount();
            } catch (Exception ignored) {
                outHeight = 0;
            }
        }
        if (outHeight > 0) {
            encOptions.put("height", outHeight);
        } else {
            encOptions.put("height", height);
        }

        outWidth = safeGetCountVal(outputWidth);
        if (outWidth <= 0) {
            try {
                outWidth = imgIn.getComponent("row").getComponentCount();
            } catch (Exception ignored) {
                outWidth = 0;
            }
        }
        if (outWidth > 0) {
            encOptions.put("width", outWidth);
        } else {
            encOptions.put("width", width);
        }
    }

    /**
     * Creates a data formatter for the corresponding codec and image size. Used for converting between raw byte arrays
     * and FFmpeg's {@link AVFrame} and {@link AVPacket} structs.
     * @param codec Video codec or uncompressed format.
     * @param width Video frame width.
     * @param height Video frame height.
     * @return Formatter object.
     * @throws ProcessException Thrown when width and height are not provided for an uncompressed format.
     */
    private AVByteFormatter getFormatter(CodecEnum codec, @Nullable Integer width, @Nullable Integer height) throws ProcessException {
        try {
            return switch (codec) {
                case RGB -> new RgbFormatter(width, height);
                case YUV -> new YuvFormatter(width, height);
                default -> new PacketFormatter();
            };
        } catch (NullPointerException e) {
            reportError("Raw formatter for " + codec + " requires non-null width and height.", e);
        }
        return null;
    }

    /**
     * @param codec Video codec or uncompressed format.
     * @return Is the codec {@link CodecEnum#RGB} or {@link CodecEnum#YUV}?
     */
    private boolean isUncompressed(CodecEnum codec) {
        return codec == CodecEnum.RGB || codec == CodecEnum.YUV;
    }

    /**
     * Accesses count value, checking for exceptions and missing data.
     * @param count {@link Count} object to access.
     * @return Value of {@code count} or 0 if not accessible.
     */
    private int safeGetCountVal(Count count) {
        int val = 0;
        if (count != null && count.hasData()) {
            try {
                val = count.getValue();
            } catch (Exception ignored) {}
        }
        return val;
    }

    /**
     * Convert an FFmpeg return code to a string and log to the console.
     * @param retCode Integer returned from an FFmpeg library function.
     */
    private void logFFmpeg(int retCode) {
        BytePointer buf = new BytePointer(AV_ERROR_MAX_STRING_SIZE);
        av_strerror(retCode, buf, buf.capacity());
        logger.warn("FFmpeg returned error code {}: {}", retCode, buf.getString());
    }

    /**
     * Set the underlying data in the output record and invoke {@link ExecutableProcessImpl#publishData()}.
     * @param frameData Output video packet/frame data.
     */
    private void publishFrameData(byte[] frameData) {

        ((DataBlockCompressed) imgOut.getData()).setUnderlyingObject(frameData.clone());
        // also copy frame timestamp
        double ts;
        if (inputTimeStamp != null && inputTimeStamp.getData() != null) {
            ts = inputTimeStamp.getData().getDoubleValue();
        } else {
            ts = System.currentTimeMillis();
        }
        outputTimeStamp.getData().setDoubleValue(ts);
        try {
            //logger.debug("Publishing");
            super.publishData();
        } catch (Exception e) {
            logger.error("Error publishing output packet", e);
            return;
        }
    }

    @Override
    public void execute() throws ProcessException
    {
        if (imgIn == null || imgIn.getData() == null || imgIn.getData().getUnderlyingObject() == null) {
            logger.warn("Input image is null");
            return;
        }
        // Start the threads if not already started
        if (!isRunning.get()) {
            startProcessThreads();
        }

        throwIfPipelineFailed();
        Pointer packet = inputFormatter.convertInput(
                ((DataBlockCompressed)imgIn.getData()).getUnderlyingObject().clone());
        boolean enqueued = false;
        try {
            enqueueInputPacket(packet);
            enqueued = true;
        } finally {
            if (!enqueued) {
                releaseInputPacket(packet);
            }
        }

    }

    void enqueueInputPacket(Pointer packet) throws ProcessException {
        synchronized (pipelineStateLock) {
            throwIfPipelineFailed();
            if (!isRunning.get() || !acceptingInput) {
                throw new ProcessException("Transcoder pipeline is not accepting input");
            }
            inputPackets.add(packet);
        }
    }

    /**
     * Runs inside a separate thread. Receives any {@link AVPacket}s or {@link AVFrame}s from the last {@link Coder}
     * in the {@link FFMpegTranscoder#videoProcs} queue, converts the struct to bytes, and publishes the data.
     * @see FFMpegTranscoder#publishFrameData(byte[])
     */
    private void outputProcess(BlockingQueue<Pointer> generationOutput,
            Coder<?, ?> generationFinalCoder, List<Coder<?, ?>> generationCoders,
            AtomicBoolean generationRun) {
        try {
            while (generationRun.get() && !Thread.currentThread().isInterrupted()) {
                Throwable workerFailure = findWorkerFailure(generationCoders);
                if (workerFailure != null) {
                    recordPipelineFailure(workerFailure);
                    return;
                }

                Pointer packet = generationOutput.poll(OUTPUT_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                if (packet != null) {
                    try {
                        publishOutputPacket(packet);
                    } finally {
                        releaseFinalOutput(generationFinalCoder, packet);
                    }
                    continue;
                }

                if (generationFinalCoder != null && generationFinalCoder.isOutputComplete()
                        && generationOutput.isEmpty()) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (generationRun.get()) {
                recordPipelineFailure(e);
            }
        } catch (Throwable t) {
            recordPipelineFailure(t);
        }
    }

    /** Converts and publishes one final-stage packet. Tests override this at the native boundary. */
    protected void publishOutputPacket(Pointer packet) {
        publishFrameData(outputFormatter.convertOutput(packet));
    }

    void throwIfPipelineFailed() throws ProcessException {
        Throwable failure = pipelineFailure.get();
        if (failure != null) {
            throw new ProcessException("FFmpeg transcoder pipeline failed", failure);
        }
    }

    private void recordPipelineFailure(Throwable failure) {
        if (failure == null) {
            return;
        }

        boolean firstFailure;
        synchronized (pipelineStateLock) {
            firstFailure = pipelineFailure.compareAndSet(null, failure);
            doRun.set(false);
            outputRun.set(false);
            acceptingInput = false;
        }
        if (!firstFailure) {
            return;
        }

        cancelWorkers();
        Thread publisher = outputThread;
        if (publisher != null && publisher != Thread.currentThread()) {
            publisher.interrupt();
        }
        logger.error("FFmpeg transcoder pipeline failed", failure);
    }

    private Throwable findWorkerFailure(List<Coder<?, ?>> coders) {
        if (coders != null) {
            for (Coder<?, ?> coder : coders) {
                if (coder.getFailure() != null) {
                    return coder.getFailure();
                }
            }
        }
        return pipelineFailure.get();
    }

    private void cancelWorkers() {
        cancelWorkers(videoProcs);
    }

    private void cancelWorkers(List<Coder<?, ?>> coders) {
        if (coders != null) {
            for (Coder<?, ?> coder : coders) {
                coder.cancel();
            }
        }
    }

    private void abortPipeline() {
        synchronized (pipelineStateLock) {
            acceptingInput = false;
            doRun.set(false);
            outputRun.set(false);
        }
        cancelWorkers();
        if (outputThread != null) {
            outputThread.interrupt();
        }
    }

    private void joinWorkersAfterCancellation(List<Coder<?, ?>> coders, long timeoutMillis)
            throws InterruptedException {
        if (coders == null) {
            return;
        }
        for (Coder<?, ?> coder : coders) {
            if (coder.isAlive() && !joinWithin(coder, timeoutMillis)) {
                logger.error("{} did not stop after cancellation", coder.getClass().getSimpleName());
            }
        }
    }

    private boolean joinWithin(Thread thread, long timeoutMillis) throws InterruptedException {
        thread.join(timeoutMillis);
        return !thread.isAlive();
    }

    private boolean allTerminated(List<Coder<?, ?>> coders, Thread publisher) {
        if (publisher != null && publisher.isAlive()) {
            return false;
        }
        for (Coder<?, ?> coder : coders) {
            if (coder.isAlive()) {
                return false;
            }
        }
        return true;
    }

    private boolean hasLivePipelineThreads() {
        return !allTerminated(videoProcs != null ? videoProcs : Collections.emptyList(),
                outputThread);
    }

    private void drainUnconsumedQueues(BlockingQueue<Pointer> generationInput,
            BlockingQueue<Pointer> generationOutput, Coder<?, ?> generationFinalCoder) {
        Pointer packet;
        if (generationInput != null) {
            while ((packet = generationInput.poll()) != null) {
                releaseInputPacket(packet);
            }
        }
        if (generationOutput != null) {
            while ((packet = generationOutput.poll()) != null) {
                releaseFinalOutput(generationFinalCoder, packet);
            }
        }
    }

    private void releaseInputPacket(Pointer packet) {
        if (packet == null) {
            return;
        }
        if (isUncompressed(inCodec)) {
            av_frame_free((AVFrame)packet);
        } else {
            av_packet_free((AVPacket)packet);
        }
    }

    private void releaseFinalOutput(Coder<?, ?> generationFinalCoder, Pointer packet) {
        if (packet != null && generationFinalCoder != null) {
            generationFinalCoder.releaseOutput(packet);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static BlockingQueue<Pointer> asPointerQueue(BlockingQueue<?> queue) {
        // Every concrete coder is constrained to AVPacket/AVFrame at construction. This adapter
        // erases only the stage-specific subtype while preserving the single shared queue object.
        return (BlockingQueue)queue;
    }

    @Override
    protected void publishData() throws InterruptedException
    {
        if (publish)
            super.publishData();
    }

    @Override
    public synchronized void dispose()
    {
        Throwable failure = null;
        try {
            // stop() first gates stale executions, cancels the base Future, and then tears down
            // the native generation. Do not duplicate a weaker lifecycle path here.
            stop();
        } catch (Throwable t) {
            failure = t;
        }

        try {
            // The base disposed state must be recorded even when native teardown timed out.
            super.dispose();
        } catch (Throwable t) {
            if (failure == null) {
                failure = t;
            } else {
                failure.addSuppressed(t);
            }
        }

        if (failure instanceof RuntimeException) {
            throw (RuntimeException)failure;
        }
        if (failure instanceof Error) {
            throw (Error)failure;
        }
        if (failure != null) {
            throw new IllegalStateException("Could not dispose transcoder pipeline", failure);
        }
    }
}
