
import DataRetrieval.DataCubeAxis;
import DataRetrieval.Dimension;
import DataRetrieval.Measures;
import DataStructure.TreeNode;

import java.util.*;
import MDXQueryProcessor.*;

public class QueryProcessor {
	public void QueryProcessor() {
	}


	public static void main(String[] args) throws Exception{
		String url="http://192.168.0.207/OLAPService/AdventureWorks.asmx";
		Dimension dm = new Dimension(url);
		Measures ms = new Measures(url);
		HashMap<Integer,String> Ms = ms.GetMeasures("Measures");
		TreeNode t = dm.GetRootDimension("Dimen");

		String paramVal ="[Dimension].[Account].[Account Number]";
		String paramName ="Hierarchy";
		String operationName = "MetaData2";
		TreeNode tr = dm.PopulateLeafNode(t,paramName,paramVal,operationName);
		tr = dm.PopulateLeafNode(tr,paramName,"[Dimension].[Account].[Account Type]",operationName);
		tr = dm.PopulateLeafNode(tr,paramName,"[Dimension].[Customer].[Country]",operationName);
		tr = dm.PopulateLeafNode(tr,paramName,"[Dimension].[Geography].[Country]",operationName);
		tr = dm.PopulateLeafNode(tr,paramName,"[Dimension].[Product].[Color]",operationName);
		tr = dm.PopulateLeafNode(tr,paramName,"[Dimension].[Employee].[Gender]",operationName);
		//problem with the below one
		//tr = dm.PopulateLeafNode(tr,paramName,"[Dimension].[Date].[Calendar Quarter of Year]",operationName);

		///query generation test: must come from user, now it is hardcoded
		List<String> hardcodedInputDim = new ArrayList<String>();
		hardcodedInputDim.add("[Dimension].[Account].[Account Number].[All Accounts]");
		hardcodedInputDim.add("[Dimension].[Customer].[Country].[All Customers]");
		hardcodedInputDim.add("[Dimension].[Account].[Account Type].[All Accounts]");
		hardcodedInputDim.add("[Dimension].[Geography].[Country].[All Geographies].[Canada]");
		hardcodedInputDim.add("[Dimension].[Employee].[Gender].[All Employees]");
		int numberofDimension = 3;// this is hardcoded now should come from user input
		int [] entryPerDimension ={2,2,1} ;
		DataCubeAxis dca = new DataCubeAxis();
		HashMap<Integer,List<TreeNode>> selectedDimension = dca.GetTreeListForEachAxis(tr,hardcodedInputDim,entryPerDimension);
		MDXQProcessor mdxq = new MDXQProcessor();
		List<String> generatedKeys= mdxq.generateKeyCombination(selectedDimension);
		int oldKeySize = generatedKeys.size();
		//mdxq.CachedKeys.put(generatedKeys.get(0),"");
		//mdxq.CachedKeys.put(generatedKeys.get(3),"");
		generatedKeys = mdxq.checkCachedKeysToRemoveDuplicateEntries(generatedKeys);
		Collections.sort(generatedKeys);
		//measures
		List<String> hardcodedInputMeasures = new ArrayList<String>();
		hardcodedInputMeasures.add("Internet Sales Amount");
		HashMap<Integer,String>selectedMeasures = ms.SetHashKeyforSelecteditems_Measures(hardcodedInputMeasures);
		mdxq.generateQueryString(selectedMeasures, selectedDimension,generatedKeys,oldKeySize);



	}




}
