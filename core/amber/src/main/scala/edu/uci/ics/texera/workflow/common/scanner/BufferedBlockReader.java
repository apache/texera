package edu.uci.ics.texera.workflow.common.scanner;

import com.google.common.primitives.Ints;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class BufferedBlockReader {
    private InputStream input;
    private long blockSize;
    private long currentPos;
    private int cursor;
    private int bufferSize = 0;
    private byte[] buffer = new byte[4096]; //4k buffer
    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private List<String> fields = new ArrayList<>();
    private HashSet<Integer> keptFields = null;
    private char delimiter;

    public BufferedBlockReader(InputStream input, long blockSize, char delimiter, int[] kept){
        this.input = input;
        this.blockSize = blockSize;
        this.delimiter = delimiter;
        if(kept != null){
            this.keptFields = new HashSet<>(Ints.asList(kept));
        }
    }

    public String[] readLine() throws IOException {
        outputStream.reset();
        fields.clear();
        int index = 0;
        while(true) {
            if (cursor >= bufferSize) {
                fillBuffer();
                if (bufferSize == -1) {
                    if(outputStream.size()>0) {
                        fields.add(outputStream.toString());
                    }
                    return fields.isEmpty() ? null: fields.toArray(new String[0]);
                }
            }
            int start = cursor;
            while (cursor < bufferSize) {
                if (buffer[cursor] == delimiter) {
                    addField(start,index);
                    outputStream.reset();
                    start = cursor+1;
                    index++;
                }else if(buffer[cursor] == '\r' || buffer[cursor] == '\n'){
                    // If line ended with '\r\n', all the fields will be outputted when buffer[cursor] == '\r'
                    // And then the cursor will move to '\n' and output Tuple(null) in next readLine() call
                    // The behavior above is the same for either
                    // 1. the current buffer keeps '\r\n'
                    // 2. '\n' comes from the next fillBuffer() call
                    addField(start,index);
                    cursor++;
                    return fields.toArray(new String[0]);
                }
                cursor++;
            }
            outputStream.write(buffer, start, bufferSize - start);
            currentPos += bufferSize - start;
        }
    }

    private void fillBuffer() throws IOException {
        bufferSize = input.read(buffer);
        cursor = 0;
    }


    private void addField(int start, int fieldIndex){
        if(keptFields == null || keptFields.contains(fieldIndex)) {
            if (cursor - start > 0) {
                outputStream.write(buffer, start, cursor - start);
                fields.add(outputStream.toString());
            } else if (outputStream.size() > 0) {
                fields.add(outputStream.toString());
            } else {
                fields.add(null);
            }
        }
        currentPos += cursor - start + 1;
    }

    public boolean hasNext() throws IOException {
        return currentPos <= blockSize && bufferSize != -1;
    }

    public void close() throws IOException {
        input.close();
    }
}
