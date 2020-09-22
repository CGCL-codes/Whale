package org.apache.whale.multicast.core;

/**
 * locate org.apache.storm.multicast.model
 * Created by master on 2019/10/11.
 */
/**
 *  Java Program to Implement Binomial Tree
 **/

import org.junit.Test;

import java.util.Scanner;

/** Class BinoNode **/
class BinoNode
{
    int data;
    int numNodes;
    BinoNode arr[];
    /** Constructor **/
    public BinoNode(int k)
    {
        data = -1;
        numNodes = k;
        arr = new BinoNode[numNodes];
    }
}

/** Class BinomialTree **/
class BinomialTree
{
    private BinoNode root;
    private int order, size;

    /** Constructor **/
    public BinomialTree(int k)
    {
        size = 0;
        order = k;
        root = new BinoNode(order);
        createTree(root);
    }
    /** Function to create a tree **/
    private void createTree(BinoNode r)
    {
        int n = r.numNodes;
        if (n == 0)
            return;
        for (int i = 0; i < n; i++)
        {
            r.arr[i] = new BinoNode(i);
            createTree(r.arr[i]);
        }
    }
    /** Function to clear tree **/
    public void clear()
    {
        size = 0;
        root = new BinoNode(order);
        createTree(root);
    }
    /** Function to check if tree is empty **/
    public boolean isEmpty()
    {
        return size == 0;
    }
    /** Function to get size of tree **/
    public int getSize()
    {
        return size;
    }
    /** Function to insert a value **/
    public void insert(int val)
    {
        try
        {
            insert(root, val);
        }
        catch (Exception e)
        {
        }
    }
    /** Function to insert a value **/
    private void insert(BinoNode r, int val) throws Exception
    {
        if (r.data == -1)
        {
            r.data = val;
            size++;
            throw new Exception("inserted !");
        }
        int n = r.numNodes;
        for (int i = 0; i < n; i++)
            insert(r.arr[i], val);
    }
    /** Function to print tree **/
    public void printTree()
    {
        System.out.print("\nBinomial Tree = ");
        printTree(root);
        System.out.println();
    }
    /** Function to print tree **/
    private void printTree(BinoNode r)
    {
        if (r.data != -1)
            System.out.print(r.data +" ");
        int n = r.numNodes;
        if (n == 0)
            return;
        for (int i = 0; i < n; i++)
            printTree(r.arr[i]);
    }
}

public class BinomialTreeTest {
    @Test
    public void testBinomialTreeTest(){
        Scanner scan = new Scanner(System.in);
        System.out.println("Binomial Tree Test\n");
        System.out.println("\nEnter order of binomial tree");
        /** Creating object of Binomial tree **/
        BinomialTree bt = new BinomialTree(scan.nextInt());
        char ch;
        /**  Perform tree operations  **/
        do
        {
            System.out.println("\nBinomial Tree Operations\n");
            System.out.println("1. insert ");
            System.out.println("2. size");
            System.out.println("3. check empty");
            System.out.println("4. clear");

            int choice = scan.nextInt();
            switch (choice)
            {
                case 1 :
                    System.out.println("Enter integer element to insert");
                    bt.insert( scan.nextInt() );
                    break;
                case 2 :
                    System.out.println("Nodes = "+ bt.getSize());
                    break;
                case 3 :
                    System.out.println("Empty status = "+ bt.isEmpty());
                    break;
                case 4 :
                    bt.clear();
                    System.out.println("\nTree Cleared\n");
                    break;
                default :
                    System.out.println("Wrong Entry \n ");
                    break;
            }
            /**  Display tree  **/
            bt.printTree();

            System.out.println("\nDo you want to continue (Type y or n) \n");
            ch = scan.next().charAt(0);
        } while (ch == 'Y'|| ch == 'y');
    }
}
