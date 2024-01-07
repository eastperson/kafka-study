package org.example.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileEventSource.class.getName());

    private boolean keepRunning = true;
    private long filePointer = 0;
    private final int updateInterval;
    private final File file;
    private final EventHandler eventHandler;

    public FileEventSource(final int updateInterval, final File file, final EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                Thread.sleep(this.updateInterval);

                // file 크기 계산
                long len = this.file.length();

                if (len < this.filePointer) {
                    LOGGER.info("file was reset as filePinter is longer than file length");
                    filePointer = len;
                    // 파일 크기가 커진 경우
                } else if (len > this.filePointer) {
                    readAppendAndSend();
                } else {
                    continue;
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage());
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        // 파일의 access point
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
        randomAccessFile.seek(this.filePointer);
        String line = null;
        while ((line = randomAccessFile.readLine()) != null) {
            sendMessage(line);
        }
        // file 이 변경되면 filePointer 와 현재의 file 의 마지막을 재설정
        this.filePointer = randomAccessFile.getFilePointer();

    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String[] tokens = line.split(",");
        String key = tokens[0];
        StringBuffer value = new StringBuffer();

        for (int i = 1; i < tokens.length; i++) {
            if (i != tokens.length - 1) {
                value.append(tokens[i] + ",");
            } else {
                value.append(tokens[i]);
            }
        }
        MessageEvent messageEvent = new MessageEvent(key, value.toString());
        this.eventHandler.onMessage(messageEvent);
    }
}
