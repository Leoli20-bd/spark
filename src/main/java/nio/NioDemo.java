package nio;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.util.HashSet;

public class  NioDemo {
    public static void main(String[] args) {
        HashSet set = new HashSet<>();
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile("/Users/haleli/myworld/test_data/action.txt", "rw");
            FileChannel fileChannel = file.getChannel();
            ByteBuffer bf = ByteBuffer.allocate(1024);
            int byteRead = fileChannel.read(bf);
            while (byteRead != -1) {
                bf.flip();
                while (bf.hasRemaining()){
                    System.out.println((char)(bf.get()));
                }
                bf.compact();
                fileChannel.close();
                //byteRead = fileChannel.read(bf);
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(file!=null){
                    file.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
