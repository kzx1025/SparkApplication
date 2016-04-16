import java.lang.Thread;
import java.lang.Exception;

/**
 * Created by root on 15-9-16.
 */
public class GetMemory {

    static class ArrayObject{
        long[][] a = new long[1024*1024][1024*1024];
        long[][] getA(){
            return this.a;
        }
        ArrayObject(){
            for(int i=0;i<1024*1024;i++){
                for(int j=0;j<1024*1024;j++){
                    a[i][j]=0;
                }
            }
        }
    }

    public static void main(String[] args){
        try{
            ArrayObject test = new ArrayObject();
            int count;
            System.out.println("start sleep.");
            Thread.sleep(20000000);
            count = test.getA().length;
            System.out.print(count);
        }catch (Exception e){
            System.out.println(e);
        }
    }
}
