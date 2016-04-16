import java.util.Scanner;

/**
 * Created by root on 15-9-18.
 */
public class MaxGift {

    public static void main(String[] args){
        int[][] gifts = new int[6][6];
        Scanner scanner = new Scanner(System.in);
        for(int i=0;i<6;i++){
            for(int j=0;j<6;j++){
                gifts[i][j] = Integer.parseInt(scanner.next());
            }
        }
        int result = getMaxGift(gifts,5,5);
        System.out.println("result:"+result);
    }

    public static int getMaxGift(int[][] dist,int x,int y){
        int max;
        if(x>0&&y>0){
            int getMax = Math.max(getMaxGift(dist,x-1,y),getMaxGift(dist,x,y-1));
            max = dist[x][y]+getMax;
        }
        else if(x==0&&y>0){
            max = dist[x][y]+getMaxGift(dist,x,y-1);
        }else if(x>0&&y==0){
            max = dist[x][y]+getMaxGift(dist,x-1,y);
        }else{
            max = dist[x][y];
        }
        return max;
    }

}
