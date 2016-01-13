package DataRetrieval;

import DataStructure.TreeNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by KheyaliMitra on 1/9/2016.
 */
public class CachedCell {
    public List<Integer> measures;
    public HashMap<Integer, List<TreeNode>> children;
    public CachedCell(){
        measures = new ArrayList<Integer>();
        children =   new HashMap<Integer, List<TreeNode>>();

    }


}
