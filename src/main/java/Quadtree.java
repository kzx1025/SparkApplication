
import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by root on 15-9-15.
 */
public class Quadtree {


    private int MAX_OBJECTS = 10000;
    private int MAX_LEVELS = 5;

    private int level;
    private List<Rectangle> objects;
    private Rectangle bounds;
    private Quadtree[] nodes;

    /*
      * Constructor
      */
    public Quadtree(int pLevel, Rectangle pBounds) {
        level = pLevel;
        objects = new ArrayList();
        bounds = pBounds;
        nodes = new Quadtree[4];
    }

    /*
  * Clears the quadtree
  */
    public void clear() {
        objects.clear();

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].clear();
                nodes[i] = null;
            }
        }
    }

    /*
    * Splits the node into 4 subnodes
    */
    private void split() {
        int subWidth = (int)(bounds.getWidth() / 2);
        int subHeight = (int)(bounds.getHeight() / 2);
        int x = (int)bounds.getX();
        int y = (int)bounds.getY();

        nodes[0] = new Quadtree(level+1, new Rectangle(x + subWidth, y, subWidth, subHeight));
        nodes[1] = new Quadtree(level+1, new Rectangle(x, y, subWidth, subHeight));
        nodes[2] = new Quadtree(level+1, new Rectangle(x, y + subHeight, subWidth, subHeight));
        nodes[3] = new Quadtree(level+1, new Rectangle(x + subWidth, y + subHeight, subWidth, subHeight));
    }

    /*
    * Determine which node the object belongs to. -1 means
    * object cannot completely fit within a child node and is part
    * of the parent node
    */
    private int getIndex(Rectangle pRect) {
        int index = -1;
        double verticalMidpoint = bounds.getX() + (bounds.getWidth() / 2);
        double horizontalMidpoint = bounds.getY() + (bounds.getHeight() / 2);

        // Object can completely fit within the top quadrants
        boolean topQuadrant = (pRect.getY() < horizontalMidpoint && pRect.getY() + pRect.getHeight() < horizontalMidpoint);
        // Object can completely fit within the bottom quadrants
        boolean bottomQuadrant = (pRect.getY() > horizontalMidpoint);

        // Object can completely fit within the left quadrants
        if (pRect.getX() < verticalMidpoint && pRect.getX() + pRect.getWidth() < verticalMidpoint) {
            if (topQuadrant) {
                index = 1;
            }
            else if (bottomQuadrant) {
                index = 2;
            }
        }
        // Object can completely fit within the right quadrants
        else if (pRect.getX() > verticalMidpoint) {
            if (topQuadrant) {
                index = 0;
            }
            else if (bottomQuadrant) {
                index = 3;
            }
        }

        return index;
    }

    /*
    * Insert the object into the quadtree. If the node
    * exceeds the capacity, it will split and add all
    * objects to their corresponding nodes.
    */
    public void insert(Rectangle pRect) {
        if (nodes[0] != null) {
            int index = getIndex(pRect);

            if (index != -1) {
                nodes[index].insert(pRect);

                return;
            }
        }

        objects.add(pRect);

        if (objects.size() > MAX_OBJECTS && level < MAX_LEVELS) {
            split();

            int i = 0;
            while (i < objects.size()) {
                int index = getIndex(objects.get(i));
                if (index != -1) {
                    nodes[index].insert(objects.remove(i));
                }
                else {
                    i++;
                }
            }
        }
    }

    /*
    * Return all objects that could collide with the given object
    */
    public List retrieve(List returnObjects, Rectangle pRect) {
        int index = getIndex(pRect);
        if (index != -1 && nodes[0] != null) {
            nodes[index].retrieve(returnObjects, pRect);
        }

        returnObjects.addAll(objects);

        return returnObjects;
    }

    public static void main(String[] args){
        Quadtree quad = new Quadtree(0, new Rectangle(0,0,10000,10000));
        quad.clear();
        long startTime = 0;
        Scanner Console=new Scanner(System.in);
        String input = Console.next();
        ArrayList<Rectangle> visited = new ArrayList<>();
        int NumT = Integer.parseInt(input);
        int[] result = new int[NumT];
        for (int i = 0; i < NumT; i++) {
            quad.clear();
            int num = Integer.parseInt(Console.next());
            int consolidateNum = 0;
            for(int j =0; j<num;j++){
                String[] rects = new String[4];
                for(int k=0;k<4;k++){
                    rects[k]=Console.next();
                }
                int Pointx = Integer.parseInt(rects[0]);
                int Pointy = Integer.parseInt(rects[3]);
                int width = Integer.parseInt(rects[2])-Pointx;
                int higth = Integer.parseInt(rects[1])-Pointy;
                Rectangle insertRect = new Rectangle(Pointx,Pointy,width,higth);
                quad.insert(insertRect);
                visited.add(insertRect);
            }
            startTime = System.currentTimeMillis();
            for(int x = 0;x<visited.size();x++){
                List<Rectangle> returnObjects = new ArrayList();
                Rectangle visitedRect = visited.get(x);
                quad.retrieve(returnObjects, visitedRect);
                System.out.println("--" + returnObjects.size());
                System.out.println("--" + returnObjects.get(0).toString());
                System.out.println("--" + returnObjects.get(1).toString());
                for (int y = 0; y < returnObjects.size(); y++) {
                    Rectangle rectb = returnObjects.get(y);
                    if(visitedRect.x >= rectb.x){
                        if(visitedRect.x-rectb.x<=rectb.width){
                            if(visitedRect.y>=rectb.y && visitedRect.y-rectb.y<=rectb.height )
                                consolidateNum ++;
                            else if(visitedRect.y<rectb.y && rectb.y-visitedRect.y<=visitedRect.height)
                                consolidateNum ++;
                        }
                    }else{
                        if(rectb.x-visitedRect.x<=visitedRect.width){
                            if(visitedRect.y>=rectb.y && visitedRect.y-rectb.y<=rectb.height )
                                consolidateNum ++;
                            else if(visitedRect.y<rectb.y && rectb.y-visitedRect.y<=visitedRect.height)
                                consolidateNum ++;
                        }
                    }
                }
                System.out.println(x+" consolidateNum:" + consolidateNum);
            }
            System.out.println("++" + consolidateNum);
            result[i]=consolidateNum/visited.size();
        }
        for(int i=0;i<NumT;i++)
          System.out.println(result[i]);
        long lastTime = System.currentTimeMillis()-startTime;
        System.out.println("time:"+((double)lastTime)/1000.0+"s");
    }
}
