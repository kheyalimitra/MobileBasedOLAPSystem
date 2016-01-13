package DataStructure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TreeNode{
    private TreeNode parent = null;
    private List children = null;
    private Object reference;
    private String hierarchyName="";
    private int _nodeCounter=0;

    public String getHierarchyName(){
        return this.hierarchyName;
    }
    public void setHierarchyName(String name)
    {
        this.hierarchyName = name;
    }
    public int getNodeCounter(){
        return this._nodeCounter;
    }

    public void setNodeCounter(int value){
        this._nodeCounter=value;
    }
    /**
     * cTtor
     * @param obj referenced object
     */
    public TreeNode(Object obj) {
        this.parent = null;
        this.reference = obj;
        this.children = new ArrayList();
        this.hierarchyName="["+obj.toString()+"]";
    }

    /**
     * cTtor
     * @param obj referenced object
     */
    public TreeNode(Object obj, String hierarchy) {
        this.parent = null;
        this.reference = obj;
        this.children = new ArrayList();
        this.hierarchyName=hierarchy;
    }

    /**
     * remove node from tree
     */
    public void remove() {
        if (parent != null) {
            parent.removeChild(this);
        }
    }

    /**
     * remove child node
     * @param child
     */
    private void removeChild(TreeNode child) {
        if (children.contains(child))
            children.remove(child);

    }

    /**
     * add child node
     * @param child node to be added
     */
    public int addChildNode(TreeNode child,int NodeCounter) {
        child.parent = this;

        if (!children.contains(child)){
            if(child.getNodeCounter()==0){
                child.setNodeCounter(NodeCounter+1);
            }
            // set hierarchy name for child node by including its ancestors name
            String childName = child.getReference().toString();
            String childHierarchyName  = this.hierarchyName + ".[" + childName + "]";
            child.setHierarchyName(childHierarchyName);
            children.add(child);
        }
        return NodeCounter+1;
    }

    /**
     * deep copy (clone)
     * @return copy of DataStructure.TreeNode
     */
    public TreeNode deepCopy() {
        TreeNode newNode = new TreeNode(reference);
        for (Iterator iter = children.iterator(); iter.hasNext();) {
            TreeNode child = (TreeNode) iter.next();
            newNode.addChildNode(child.deepCopy(),child.getNodeCounter());
        }
        return newNode;
    }
    /**
     * @return level = distance from root
     */
    public int getLevel() {
        int level = 0;
        TreeNode p = parent;
        while (p != null) {
            ++level;
            p = p.parent;
        }
        return level;
    }

    /**
     * walk through subtree of this node
     * @param callbackHandler function called on iteration
     */
    public int walkTree(TreeNodeCallback callbackHandler) {
        int code = 0;
        code = callbackHandler.handleTreeNode(this);
        if (code != TreeNodeCallback.CONTINUE)
            return code;
        ChildLoop: for (Iterator iter = children.iterator(); iter.hasNext();) {
            TreeNode child = (TreeNode) iter.next();
            code = child.walkTree(callbackHandler);
            if (code >= TreeNodeCallback.CONTINUE_PARENT)
                return code;
        }
        return code;
    }

    /**
     * walk through children subtrees of this node
     * @param callbackHandler function called on iteration
     */
    public int walkChildren(TreeNodeCallback callbackHandler) {
        int code = 0;
        ChildLoop: for (Iterator iter = children.iterator(); iter.hasNext();) {
            TreeNode child = (TreeNode) iter.next();
            code = callbackHandler.handleTreeNode(child);
            if (code >= TreeNodeCallback.CONTINUE_PARENT)
                return code;
            if (code == TreeNodeCallback.CONTINUE) {
                code = child.walkChildren(callbackHandler);
                if (code > TreeNodeCallback.CONTINUE_PARENT)
                    return code;
            }
        }
        return code;
    }

    /**
     * @return List of children
     */
    public List getChildren() {
        return children;
    }

    /**
     * @return parent node
     */
    public TreeNode getParent() {
        return parent;
    }

    /**
     * @return reference object
     */
    public Object getReference() {
        return reference;
    }

    /**
     * set reference object
     * @param object reference
     */
    public void setReference(Object object) {
        reference = object;
    }

    public TreeNode getSubTree(Object obj)
    {
        for(int i=0;i<children.size();i++) {
            TreeNode node = (TreeNode) children.get(i);
            if (node.reference.equals(obj)) {
                return node;
            }
        }
        return null;
    }
}
interface TreeNodeCallback {

    public static final int CONTINUE = 0;
    public static final int CONTINUE_SIBLING = 1;
    public static final int CONTINUE_PARENT = 2;
    public static final int BREAK = 3;

    /**
     * @param node the current node to handle
     * @return 0 continue tree walk
     *         1 break this node (continue sibling)
     *         2 break this level (continue parent level)
     *         3 break tree walk
     */
    int handleTreeNode(TreeNode node);
} // DataStructure.TreeNodeCallback

