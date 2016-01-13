package MDXQueryProcessor;

import DataRetrieval.CachedCell;
import DataStructure.TreeNode;
import com.sun.deploy.util.ArrayUtil;
import com.sun.deploy.util.StringUtils;
import com.sun.xml.internal.bind.v2.runtime.SwaRefAdapter;
import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader;
import com.sun.xml.internal.ws.commons.xmlutil.Converter;
import sun.reflect.generics.tree.Tree;

import java.util.*;

/**
 * Created by KheyaliMitra on 1/5/2016.
 */
public class MDXQProcessor {
    //this is the cached keys from previous queries
    public HashMap<String, CachedCell> CachedKeys =  new HashMap<String,CachedCell>();
    //compressed keys after sort and merge
    private List<String> compressedKeys =  new ArrayList<String>();
    private boolean dataDownloaded; // the data for the current level is downloaded?
    private boolean leafDataReady;  // the data for the leaf level is downloaded?

    /**
     * Generates key combinations for selected and un cached dimension entries for query
     * @param keys
     * @return
     */
    public List<String> generateKeyCombination(HashMap<Integer,List<TreeNode>> keys){
        List<String> result= new ArrayList<String>();

        //recursively call key generator starting from 0
        result=_callKeyGenerator(0,keys, result);

       return result;

    }

    /**
     * Checks user query entry from cache, if found, removes the entry to be fetched from server
     * @param keys
     * @return
     */
    public List<String> checkCachedKeysToRemoveDuplicateEntries(List<String>keys)
    {
        for(int i=0;i<keys.size();i++)
        {
            if(this.CachedKeys.containsKey(keys.get(i)))
            {
                keys.remove(i--);// to reset the index field for iteration
            }
        }
        return  keys;

    }
    private String _generateQueryForMeasures(HashMap<Integer,String>measures){
        String sqlStatement = "select {";
        List<String> members=new ArrayList<String>();

        for (int i = 0; i < measures.values().size(); i++) {
            String val =  measures.values().toArray()[i].toString();
            val = val.substring(val.indexOf(".")+1);//remove root name [Measures]
            members.add("[Measures].[" + val + "]");
        }
        sqlStatement += String.join(",",members) + "} on axis(0), ";
        return  sqlStatement;
    }

    /**
     * Generate query which is used to fetch data from server
     * @param selectedDimension
     * @param filteredKeys
     * @param oldKeySize
     */
    public void generateQueryString(HashMap<Integer,String> measures,HashMap<Integer,List<TreeNode>> selectedDimension, List<String>filteredKeys, int oldKeySize){

        List<String> queries= new ArrayList<String>();
        List<String>queries2 = new ArrayList<String>();
        String sqlStatement = _generateQueryForMeasures(measures);

        // if none of the entries are cached
        if(filteredKeys.size()==oldKeySize)
        {
           /* //sort the keys
            Collections.sort(filteredKeys);
            List<TreeNode> xSet =  _fillaxisArray(selectedDimension, 0);
            List<TreeNode> ySet = _fillaxisArray(selectedDimension, 1);
            List<TreeNode> zSet =  _fillaxisArray(selectedDimension, 2);
            TreeNode xAxisNode = xSet.get(0).getParent();
            TreeNode yAxisNode = ySet.get(1).getParent();
            TreeNode zAxisNode = zSet.get(2).getParent();
            //Check if its  immediate parent is cached or not
            String parentNodeKey = this._findParentCellforSelectedDimensions(selectedDimension);
            //we have found its parent in cache
            if(parentNodeKey!=null && parentNodeKey!="")
            {

            }
            // need to download the data from the server side
            else
            {*/
                dataDownloaded =false;
                leafDataReady = false;
                compressedKeys = filteredKeys;
                _compressFilteredKeys(compressedKeys);
                HashMap<Integer,TreeNode>keyValPairs = _getKeyValuePairOfSelectedDimensionsFromTree(selectedDimension);
                List<List<String>> subHierarchy_Leaf = _generateSubHierarchies(filteredKeys,keyValPairs,true);
                /// generating the sub-hierarchies for downloading the current level nodes
                List<List<String>> subHierarchy = _generateSubHierarchies(filteredKeys,keyValPairs,false);
                for (int i = 0; i < subHierarchy_Leaf.size(); ++i)
                {
                    String subQuery = sqlStatement;
                    subQuery = _generateSubQuery(subHierarchy_Leaf.get(i), subQuery);
                    queries.add(subQuery);
                    String subQuery2 = sqlStatement;
                    subQuery2 = _generateSubQuery(subHierarchy.get(i), subQuery2);
                    queries2.add(subQuery2);
                }
            //}
            //download data

        }
         //No keys are cached : so either its parent keys are cached or has to down load data from server

    }
    // generating sub-query
    private String _generateSubQuery(List<String> hierarchies, String query) {
        query += String.join(",",hierarchies);
        query += "from [Adventure Works]";
        query.replace("&", "ampersand");
        return query;
    }
    private HashMap<Integer,TreeNode> _getKeyValuePairOfSelectedDimensionsFromTree(HashMap<Integer,List<TreeNode>>dimensionTree)
    {
        HashMap<Integer,TreeNode>keyValPairs =  new HashMap<Integer, TreeNode>();
        for(int i=0;i<dimensionTree.size();i++)
        {
            List<TreeNode>chidren = dimensionTree.get(i);
            //Iterate through JSON Object to generate parent and its child nodes and then finally attach to its root node.
            Iterator nodeIterator = chidren.iterator();
            while (nodeIterator.hasNext()) {
                TreeNode node =(TreeNode) nodeIterator.next();

                keyValPairs.put(node.getNodeCounter(),node);//+"#"+i
            }
        }
        return keyValPairs;
    }
    private List<List<String>> _generateSubHierarchies(List<String>selectedKeys,HashMap<Integer,TreeNode> keyValPairsForDimension, boolean isLeafLevel){
        List<List<String>> subHierarchies = new ArrayList<List<String>>();
        // example of keys: "194,195#201,202"
        // Hierarchy Selection:
        // [Customer].[Country].[All Customers].[Australia] --> 194
        // [Customer].[Country].[All Customers].[Canada] --> 195
        // [Product].[Color].[All Products].[Black] --> 201
        // [Product].[Color].[All Products].[Blue] --> 202
        for (int i = 0; i < selectedKeys.size(); ++i) {
            String[] subKeys = selectedKeys.get(i).split("#");
            List<String> hierarchies = new ArrayList<String >(); // the hirarchies for a single sub-query
            for (int j = 0; j < subKeys.length; ++j) {
                List<String> members = new ArrayList<String>(); //
                String[] subSubKeys = subKeys[j].split(","); // the keys for a single dimension in a single sub-query
                TreeNode node=new TreeNode("");
                for (int k = 0; k < subSubKeys.length; ++k) {
                    node = keyValPairsForDimension.get(Integer.parseInt(subSubKeys[k]));
                    String nodeName=node.getHierarchyName();
                    nodeName = nodeName.substring(nodeName.indexOf(".")+1);// remove root node name<Dimension>
                    members.add(nodeName);
                }
                // generate entries for lef level entries
                if(isLeafLevel) {
                    TreeNode parentNode = node.getParent();
                    int distance = parentNode.getLevel() - (node.getLevel() - 2);
                    hierarchies.add("DESCENDANTS({" + String.join(",", members) + "},"
                            + distance + ",LEAVES) on axis(" + (j + 1) + ") ");
                }
                // generate entries for current levels
                else
                {
                   hierarchies.add("{" + String.join(",", members) + "} on axis(" + (j + 1) + ") ");

                }
            }
            subHierarchies.add(hierarchies);
        }
        return subHierarchies;
    }
    /**
     * This is the main method to optimise query cells. It compresses all un cached keys using sort and merge method
     * @param keys
     * @return
     */
    private void _compressFilteredKeys(List<String>keys)
    {
        _sortAndMergeKeys(keys);

    }
    private void _extractDataFromParentCellInCache(String key, HashMap<Integer,List<TreeNode>> selectedDimension,
                                                   int rowAxisNode, int rowLevelToFetch, int columnAxisNode,
                                                   int columnLevelToFetch, int answerInGetMatrix)
    {

       CachedCell parentCell = CachedKeys.get(key);
        if(parentCell!=null){
            List<String> measures =    parentCell.measures;
            HashMap<String,List<String>> children = parentCell.children;
           // children.get(0).
        }
    }
    /**
     *
     * @param selectedDimension
     */
    private String _findParentCellforSelectedDimensions(HashMap<Integer,List<TreeNode>> selectedDimension)
    {
        return _findCellEntriesRecursively(selectedDimension,0,0);
    }

    private String  _findCellEntriesRecursively(HashMap<Integer,List<TreeNode>> selectedDimension, int start, int axis)
    {
        List<TreeNode> currentNodeList = selectedDimension.get(axis);
        String key;
        while(selectedDimension.get(axis).get(start)!=null)
        {
            int length =selectedDimension.get(axis).size();
            //reaches at leaf node
            if(start == length-1) {
                key = _generateKeys(selectedDimension.get(axis));
                if (this.CachedKeys.containsKey(key)) {
                    return key;
                }
            }
            else if(start< length-1)
                {
                    key = this._findCellEntriesRecursively(selectedDimension,start+1,axis);
                    if(key!=null)
                    {
                        return key;
                    }
                }
            if(selectedDimension.get(axis).get(start)!=null) {
                selectedDimension.get(axis).set(start, selectedDimension.get(axis).get(start).getParent());
            }
            else
            {
                break;
            }
        }
        selectedDimension.get(axis).set(start,currentNodeList.get(start));
        return null;
    }
    private String _generateKeys(List<TreeNode>Nodes)
    {
        String key = "";
        for (int i = 0; i < Nodes.size(); ++i) {
            if (key == "") {
                key += Nodes.get(i).getNodeCounter();
            } else {
                key += ("#" + Nodes.get(i).getNodeCounter());
            }
        }
        return key;
    }
    /**
     * Sorts and merges every query cells and optimizes number of entries per axis
     * @param keys
     */
    private void _sortAndMergeKeys(List<String >keys)
    {
        List<String> mergedKeys=new ArrayList<String>();
        int flag=0;
        for(int i=0;i<keys.size()-1;i++)
        {
            String[] keys1 = keys.get(i).split("#");
            String[] keys2 = keys.get(i+1).split("#");

            // validates keys if they are candidate for merging,The two cells can only be merged if only one of their ids differ in the same dimension.
            int index = _validateKeys(keys1, keys2);
            // if not valid
            if (index != -1) {
                // combine keys with ',' separation
                String[] combinedKeys = _combineKeys(keys1, keys2, index);
                // replace old one with new one and remove i+1 th one since it is already merged.
                keys.set(i, StringUtils.join(Arrays.asList(combinedKeys), "#"));
                keys.remove(i+1);// remove the immediate next element which is got merged with ith element
                compressedKeys = keys;
                flag = 1;
            }
        }
        if(flag==1)
        {
            _sortAndMergeKeys(keys);
        }

    }

    /**
     *
     * @param selectedDimension
     * @param axisIndex
     * @return
     */
    private  List<TreeNode> _fillaxisArray(HashMap<Integer, List<TreeNode>> selectedDimension, int axisIndex) {
        List<TreeNode> set =  new ArrayList<TreeNode>();
        for(int i=0;i<selectedDimension.get(axisIndex).size();i++)
        {
            set.add(selectedDimension.get(axisIndex).get(i));
        }
        return set;
    }

    /**
     * combined two entries from selected dimensions with ','
     * @param keys1
     * @param keys2
     * @param index
     * @return
     */
    private String[] _combineKeys(String[] keys1, String[] keys2, int index)
    {
        String[] subKeys1 = keys1[index].split(",");
        String[]  subKeys2 = keys2[index].split(",");
        String[] mergedKeys =new  String[subKeys1.length+subKeys2.length];
        System.arraycopy(subKeys1,0,mergedKeys,0,subKeys1.length);
        System.arraycopy(subKeys2,0,mergedKeys,subKeys1.length,subKeys2.length);
        Arrays.sort(mergedKeys);
        String newKey = StringUtils.join(Arrays.asList(mergedKeys),",");
        keys1[index] = newKey;
        return  keys1;
    }

    /**
     * validates two keys if they can be combined: are candidate for merging,The two cells can only be merged
     * if only one of their ids differ in the same dimension.
     * @param keys1
     * @param keys2
     * @return
     */
    private int _validateKeys(String[] keys1, String[] keys2)
    {
        int count=0;
        int index =-1;
        for(int i=0;i<keys1.length;i++)
        {
            if(!keys1[i].equals(keys2[i])){
                count++;
                if(count==1)// checking for only 1 mismatch
                {
                    index =i;
                }
            }

        }
        //not a candidate for merging, these 2 keys differs in more than 1 way
        if(count>1)
        {
            index=-1;
        }

        return  index;
    }

    /**
     * recursively generates keys for un cached dimension entries for query
     * @param axisIndex
     * @param keys
     * @param result
     * @return
     */
    private List<String> _callKeyGenerator(int axisIndex, HashMap<Integer, List<TreeNode>> keys, List<String> result) {
        //end condition
        if(axisIndex>=keys.size())
        {
            return result;
        }

        List<String> newResult = new ArrayList<String>();
        // loop through each axis entry
        for(int i=0;i<keys.get(axisIndex).size();i++)
        {
            // if this is the 1st recursion, no saved keys in result:
            if(result.size()==0)
            {
                newResult.add(String.valueOf( keys.get(axisIndex).get(i).getNodeCounter()) );// can not implicitly convert int to String
            }
            else
            {
                //for every save entry in result list , append new ones
                for(int j=0;j<result.size();j++)
                {
                    newResult.add(result.get(j)+"#"+String.valueOf( keys.get(axisIndex).get(i).getNodeCounter()));
                }
            }

        }
        result = newResult;
        return  _callKeyGenerator(axisIndex+1,keys,result);
    }
    private void _extractData(String key, List<TreeNode> value, TreeNode rowAxisNode, int rowLevelToFetch,
                              TreeNode columnAxisNode, int columnLevelToFetch, answerInGetMatrix) {
        CachedCell cell = CachedKeys.get(key);
        if (cell != null) {
            List<Integer> measures = cell.measures;
            HashMap<Integer, List<TreeNode>> children = cell.children;
            int index = 0;
            for (int j = 0; j < children.get(1).size(); ++j) {
                if (this._isAncestorCol(value, children.get(1).get(j))) {
                    for (int i = 0; i < children.get(0).size(); ++i) {
                        if (this._isAncestorRow(value, children.get(0).get(i))) {
                            int[] current ={i, j};
                            int[] dim = new int[2];
                            dim[0] = children.get(0).size();
                            dim[1] = children.get(1).size();
                            int cellOrdinal = this._calculateCellOrdinal(dim, current);
                            int k;
                            for (k = index; k < measures.size(); ++k) {
                                if (measures.get(k) == cellOrdinal) {
                                    index = k + 1;
                                    break;
                                }
                                if (measures.get(k) > cellOrdinal) {
                                    index = k;
                                    k = -1;
                                    break;
                                }
                            }

                            TreeNode rowMemberNode = children.get(0).get(i);
                            TreeNode colMemberNode = children.get(1).get(j);

                            int row;
                            int col;

                            // Set row/column to the row/column ancestors at the target aggregation level
                            if (rowMemberNode.getLevel() <= rowAxisNode.getLevel() + rowLevelToFetch) {
                                row = rowMemberNode.getNodeCounter();
                            } else {
                                row = rowMemberNode.getParent().getNodeCounter();//(rowAxisNode.depth + rowLevelToFetch).data.memberKey;
                            }
                            if (colMemberNode.getLevel() <= columnAxisNode.getLevel() + columnLevelToFetch) {
                                col = colMemberNode.getNodeCounter();//.data.memberKey;
                            } else {
                                col = colMemberNode.getParent().getNodeCounter();//getAncestor(columnAxisNode.depth + columnLevelToFetch).data.memberKey;
                            }

                            if (k > -1 && k < measures.size()) {
                                // If row/column exists in the answer, aggregate the tuple measure
                               /* if (answerInGetMatrix.hasOwnProperty(row)) {
                                    if (answerInGetMatrix[row].hasOwnProperty(col)) {
                                        answerInGetMatrix[row][col] += measures[k][0];
                                    }
                                }*/
                            }
                        }
                    }
                }
            }
        }
    }
    private int _calculateCellOrdinal (int[] dim, int[] current) {
        int E = 1;
        int cellOrdinal = current[0];
        for (int i = 1; i < dim.length; i++) {
            E = E * dim[i-1];
            cellOrdinal = cellOrdinal + current[i] * E;
        }
        return cellOrdinal;
    }
    private boolean _isAncestorRow(List<TreeNode>group, TreeNode node) {
        for (int i = 0; i < group.size(); ++i) {
            if (this._isAncestor(group.get(i), node)){
                return true;
            }
        }
        return false;
    }
    private boolean _isAncestorCol(List<TreeNode> group, TreeNode node) {
        for (int i = 0; i < group.size(); ++i) {
            if (this._isAncestor(group.get(i), node)) {
                return true;
            }
        }
        return false;
    }
    private boolean _isAncestor(TreeNode AxisNode, TreeNode MemberNode) {
        TreeNode node = MemberNode;
        int depth = MemberNode.getLevel();
        for (int i = 0; i < depth; i++) {
            if ( node.getLevel()== AxisNode.getLevel())
                return true;
            node = node.getParent();
        }
        return false;

    }
}
