package DataRetrieval;

import DataStructure.TreeNode;

import java.awt.*;
import java.util.HashMap;
import java.util.List;

/**
 * Created by KheyaliMitra on 1/5/2016.
 */
public class DataCubeAxis {

    /**
     * Put all dimensions in each axis based on input array entryPerAxis
     * @param tree
     * @param dimensionList
     * @param entryPerAxis
     * @return
     */
    public HashMap<Integer,List<TreeNode>> GetTreeListForEachAxis(TreeNode tree, List<String> dimensionList, int[] entryPerAxis){
        //Gets the axis size
        int numberOfAxis = entryPerAxis.length;
        //Gets total dimensions
        int totalDimensions = dimensionList.size();

        HashMap<Integer,List<TreeNode>> selectedDimen= new HashMap<Integer,List<TreeNode>>();
        int start=0;
        Dimension dm = new Dimension();
        for( int i=0;i<numberOfAxis;i++) {
            selectedDimen.put(i,dm.GetTreeListFromDimensionList(tree, dimensionList.subList(start,start+ entryPerAxis[i])));
            start += entryPerAxis[i];
        }
        return selectedDimen;
    }
}
