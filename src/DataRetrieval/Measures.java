package DataRetrieval;
import org.json.simple.parser.JSONParser;

import java.util.*;

/**
 * Created by KheyaliMitra on 1/3/2016.
 */
public class Measures {
    private String _soap_Address ;//"http://webolap.cmpt.sfu.ca/ElaWebService/Service.asmx";
    private int _nodeCounter=0;
    private   HashMap<Integer,String> measureList;
    /**
     * Class constructor
     * @param URL
     */
    public Measures(String URL){
        this._soap_Address = URL;
    }
    /***
     * Get measures list form database after query to the server
     * @return a hash table containing unique number and string value (measure names)
     * @throws Exception
     */
    public HashMap<Integer,String> GetMeasures(String OperationName) throws Exception {
        String measureString = this._getJSONString(OperationName);

        //create JSON parser object
        JSONParser parser = new JSONParser();
        //parse dimension string returned SOAP
        Object jsonObject = parser.parse(measureString);
        // set value to hash map from json object
        measureList=  new HashMap<Integer, String>();
        //call method to iterate through json object and populate list
        measureList = _generateMeasureList(jsonObject);
        return measureList;
    }

    /**
     * Iterates through json object and populates hash map
     * @param jsonObject
     * @return hashmap <int, string>
     */
    private HashMap<Integer,String> _generateMeasureList(Object jsonObject) {
        HashMap<Integer, String> measures = new HashMap<>();
        // convert json object into list of strings
        List<String> listDetails = ( List<String>) jsonObject;
        Iterator iterator = listDetails.iterator();
        while (iterator.hasNext()) {
            //add entry of each measures in measures hash map
           measures.put(_nodeCounter, iterator.next().toString().split("\\|")[1]);
            _nodeCounter++;
        }
        return measures;
    }

    /**
     * Generates json string to call web method which will fetch content from server
     * @param operationName
     * @return
     * @throws Exception
     */
    private String _getJSONString(String operationName) throws Exception{
        //Creates soap retrieval object
        Soap soapRetrieval = new Soap();

        //set parameters to make it usable for both root node (parameters=null ) and child nodes (key,value pairs)
        HashMap<String,Object> parameters =null;
        //call getJSON String for root and child tree generation
        String measuresString = soapRetrieval.GetJSONString(this._soap_Address, operationName, null);
        return measuresString;
    }
    public HashMap<Integer,String> SetHashKeyforSelecteditems_Measures(List<String>hardcodedInputMeasures){
        HashMap<Integer,String> hm  = new HashMap<Integer, String>();
        Map<Integer,String> treemap = this.measureList;
        Set<Integer> s;
        for (Map.Entry<Integer, String> entry : treemap.entrySet()) {
            for(int i=0;i<hardcodedInputMeasures.size();i++)
            {
                String val = entry.getValue();
                if (Objects.equals(hardcodedInputMeasures.get(i), val)) {
                    int key = entry.getKey();
                    hm.put(key,hardcodedInputMeasures.get(i));
                }
            }
            break;
        }
        return hm;
    }
}
