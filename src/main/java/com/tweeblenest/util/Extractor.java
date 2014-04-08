package com.tweeblenest.util;

import arq.update;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Created with IntelliJ IDEA.
 * User: Geeky_Vin
 * Date: 11/23/13
 * Time: 12:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class Extractor {

    public static void main(String args[]){

        Extractor ext = new Extractor();
        ext.deleteAll();
        ext.extractAutoMakeDetails();

    }

    void extractAutoMakeDetails()  {
        String INPUT_QUERY = "[{ \n"+
                                              "  \"id\": null,\n"+
                                              "  \"/type/object/name\": null, \n"+
                                              "  \"mid\": null, \n"+
                                              "  \"type\": \"/automotive/company\", \n"+
                                              "  \"/automotive/company/make_s\": [{ \n"+
                                              "       \"/type/object/name\": null,\n"+
                                              "       \"mid\": null,\n"+
                                              "        \"/automotive/make/model_s\": [{\n"+
                                              "           \"mid\": null,\n"+
                                              "           \"/type/object/name\": null\n"+
                                              "        }]\n"+
                                              "   }] \n"+
                                              "}]\n";

        String result = requestFreebase(INPUT_QUERY);
        System.out.println(result);
        try {
            JSONParser parser = new JSONParser();
            JSONObject json = null;
            json = (JSONObject) parser.parse(result);
            System.out.println(json);
            JSONArray resultObj = (JSONArray) json.get("result");

            StringBuffer comp_mid = new StringBuffer();
            StringBuffer comp_name = new StringBuffer();

            StringBuffer make_mid = new StringBuffer();
            StringBuffer make_name = new StringBuffer();

            StringBuffer auto_model_mid = new StringBuffer();
            StringBuffer auto_model_name = new StringBuffer();

            Model model= ModelFactory.createDefaultModel();

            for(Object obj : resultObj){
                JSONObject comp_json_obj = (JSONObject) obj;
                comp_mid.append("ns:"+comp_json_obj.get("mid").toString().substring(1).replace("/","."));
                comp_name.append(comp_json_obj.get("/type/object/name").toString());

                insertToFusekiServer(model.createResource(comp_mid.toString()), model.createResource("ns:type.object.name"), model.createTypedLiteral(comp_name.toString()));
                insertToFusekiServer(model.createResource(comp_mid.toString()), model.createResource("ns:type"), model.createResource("ns:automotive.company"));

                JSONArray make_json_array = (JSONArray) comp_json_obj.get("/automotive/company/make_s");

                for(Object make_obj: make_json_array){
                    JSONObject make_json_obj = (JSONObject) make_obj;
                    make_mid.append("ns:"+make_json_obj.get("mid").toString().substring(1).replace("/","."));
                    make_name.append(make_json_obj.get("/type/object/name").toString());
                    insertToFusekiServer(model.createResource(comp_mid.toString()), model.createResource("ns:automotive.company.make_s"), model.createResource(make_mid.toString()));
                    insertToFusekiServer(model.createResource(make_mid.toString()), model.createResource("ns:type.object.name"), model.createTypedLiteral(make_name.toString()));
                    insertToFusekiServer(model.createResource(make_mid.toString()), model.createResource("ns:type"), model.createResource("ns:automotive.make"));

                    JSONArray model_json_array = (JSONArray) make_json_obj.get("/automotive/make/model_s");

                    for(Object model_obj : model_json_array){
                        JSONObject model_json_obj = (JSONObject) model_obj;
                        auto_model_mid.append("ns:"+model_json_obj.get("mid").toString().substring(1).replace("/","."));
                        auto_model_name.append(model_json_obj.get("/type/object/name").toString());
                        insertToFusekiServer(model.createResource(auto_model_mid.toString()), model.createResource("ns:automotive.model.make"), model.createResource(make_mid.toString()));
                        insertToFusekiServer(model.createResource(auto_model_mid.toString()), model.createResource("ns:type.object.name"), model.createTypedLiteral(auto_model_name.toString()));
                        insertToFusekiServer(model.createResource(auto_model_mid.toString()), model.createResource("ns:type"), model.createResource("ns:automotive.model"));
                        auto_model_mid.delete(0,auto_model_mid.length());
                        auto_model_name.delete(0,auto_model_name.length());
                    }

                    make_mid.delete(0,make_mid.length());
                    make_name.delete(0,make_name.length());
                }

                comp_mid.delete(0,comp_mid.length());
                comp_name.delete(0,comp_name.length());
            }

        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    String requestFreebase(String query){
        try{
            HttpTransport httpTransport = new HttpTransport();
            System.out.println("**************************Query************************** \n" +
             query+
            "\n***************************************************************");

            GenericUrl url = new GenericUrl("https://www.googleapis.com/freebase/v1/mqlread");
            url.put("key", "AIzaSyDxi65hXZJgFeXXJOKg9NgSDzQ2BoQWRas");
            url.put("query", query);
            HttpRequest request = httpTransport.buildGetRequest();
            request.setUrl(url.toString());
            HttpResponse httpResponse = request.execute();
            String result = httpResponse.parseAsString();
            return result;
        }catch(Exception e){
            e.printStackTrace();
            return e.toString();
        }
    }

    static String getDataSet(){
        String loc = "http://192.168.1.4:3030/triples";
        return loc;
    }

    void insertToFusekiServer(RDFNode s, RDFNode p, RDFNode o) {

        String INSERT_QUERY =
                "PREFIX key: <http://rdf.freebase.com/key/> " +
                        "PREFIX ns: <http://rdf.freebase.com/ns/> " +
                        "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
                        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
                        + " INSERT DATA"
                        + " {"
                        + " ?s ?p ?o ."
                        +" }";
        final ParameterizedSparqlString pss = new ParameterizedSparqlString( INSERT_QUERY );
        pss.setParam("s", s);
        pss.setParam("p", p);
        pss.setParam("o", o);
        pss.setNsPrefix("ns", "http://rdf.freebase.com/ns/");
        pss.setNsPrefix("rdfs","http://www.w3.org/2000/01/rdf-schema#");
        //QueryExecution qe = QueryExecutionFactory.sparqlService("http://192.168.1.4:3030/triples/sparql", pss.toString());
        UpdateRequest q = UpdateFactory.create(pss.toString());
        UpdateExecutionFactory.createRemote(q, getDataSet()+"/ update ").execute();

    }

    void deleteAll(){
        String DELETE_QUERY =   "PREFIX key: <http://rdf.freebase.com/key/> " +
                "PREFIX ns: <http://rdf.freebase.com/ns/> " +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
                + " DELETE"
                + " {"
                + " ?s ?p ?o ."
                +" }" +
                "WHERE" +
                "{" +
                "?s ?p ?o ." +
                "}";
        final ParameterizedSparqlString pss = new ParameterizedSparqlString( DELETE_QUERY );
        pss.setNsPrefix("ns","http://rdf.freebase.com/ns/");
        pss.setNsPrefix("rdfs","http://www.w3.org/2000/01/rdf-schema#");
        //QueryExecution qe = QueryExecutionFactory.sparqlService("http://192.168.1.4:3030/triples/sparql", pss.toString());
        UpdateRequest q = UpdateFactory.create(pss.toString());
        UpdateExecutionFactory.createRemote(q, getDataSet()+"/update").execute();

    }
}
