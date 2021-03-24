/*
 * #%L
 * ACS AEM Tools Bundle
 * %%
 * Copyright (C) 2015 Adobe
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.adobe.acs.tools.csv_resource_type_updater.impl;

import com.adobe.acs.tools.csv.impl.CsvUtil;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import com.day.cq.dam.api.Asset;
import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.sling.SlingServlet;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.*;
import org.apache.sling.api.servlets.SlingAllMethodsServlet;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@SlingServlet(
        label = "ACS AEM Tools - CSV Resource Type Updater Servlet",
        methods = { "POST" },
        resourceTypes = { "acs-tools/components/csv-resource-type-updater" },
        selectors = { "update" },
        extensions = { "json" }
)
public class CsvResourceTypeUpdateServlet extends SlingAllMethodsServlet {
    private static final Logger log = LoggerFactory.getLogger(CsvResourceTypeUpdateServlet.class);

    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final Pattern DECIMAL_REGEX = Pattern.compile("-?\\d+\\.\\d+");

    // 3 to account for Line Termination
    private static final int VALID_ROW_LENGTH = 3;
    private JSONObject propertiesObject = null;

    @Override
    protected final void doPost(final SlingHttpServletRequest request, final SlingHttpServletResponse response)
            throws IOException {

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        final JSONObject jsonResponse = new JSONObject();
        final Parameters params = new Parameters(request);

        if (params.getFile() != null) {

            final long start = System.currentTimeMillis();
            final Iterator<String[]> rows = CsvUtil.getRowsFromCsv(params);

            try {
                request.getResourceResolver().adaptTo(Session.class).getWorkspace().getObservationManager().setUserData("acs-aem-tools.csv-resource-type-updater");
                propertiesObject  = getPropertiesMappingJsonFile(request);
                final Result result = this.update(request.getResourceResolver(), params, rows);

                if (log.isInfoEnabled()) {
                    log.info("Updated as TOTAL of [ {} ] resources in {} ms", result.getSuccess().size(),
                            System.currentTimeMillis() - start);
                }

                try {
                    jsonResponse.put("success", result.getSuccess());
                    jsonResponse.put("failure", result.getFailure());
                } catch (JSONException e) {
                    log.error("Could not serialized results into JSON", e);
                    this.addMessage(jsonResponse, "Could not serialized results into JSON");
                    response.setStatus(SlingHttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                }
            } catch (Exception e) {
                log.error("Could not process CSV type update replacement", e);
                this.addMessage(jsonResponse, "Could not process CSV type update. " + e.getMessage());
                response.setStatus(SlingHttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        } else {
            log.error("Could not find CSV file in request.");
            this.addMessage(jsonResponse, "CSV file is missing");
            response.setStatus(SlingHttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        response.getWriter().print(jsonResponse.toString());
    }

    /**
     * Update all resources that have matching property values with the new values in the CSV.
     *
     * @param resourceResolver the resource resolver object
     * @param params           the request params
     * @param rows             the CSV rows
     * @return a list of the resource paths updated
     * @throws PersistenceException
     */
    private Result update(final ResourceResolver resourceResolver,
                          final Parameters params,
                          final Iterator<String[]> rows) throws PersistenceException, JSONException {

        final Result result = new Result();

        final Map<String, String> map = new HashMap<String, String>();

        while (rows.hasNext()) {
            String[] row = rows.next();

            if (row.length == VALID_ROW_LENGTH) {
                map.put(row[0], row[1]);
                log.debug("Adding type translation [ {} ] ~> [ {} ]", row[0], row[1]);
            } else {
                log.warn("Row {} is malformed", Arrays.asList(row));
            }
        }

        String query = "SELECT * FROM [nt:base] WHERE ";
        query += "ISDESCENDANTNODE([" + params.getPath() + "]) AND (";

        final List<String> conditions = new ArrayList<String>();

        for (String key : map.keySet()) {
            conditions.add("[" + params.getPropertyName() + "] = '" + key + "'");
        }

        query += StringUtils.join(conditions, " OR ");
        query += ")";

        log.debug("Query: {}", query);

        final Iterator<Resource> resources = resourceResolver.findResources(query, "JCR-SQL2");

        int count = 0;
        while (resources.hasNext()) {
            final Resource resource = resources.next();
            final ModifiableValueMap properties = resource.adaptTo(ModifiableValueMap.class);
            String oldResourceType = properties.get(params.getPropertyName(), String.class);
            String newValue = map.get(oldResourceType);


            if (newValue != null) {
                try {
                    JSONObject obj;
                    if (null != propertiesObject) {
                        try {
                            obj = propertiesObject.getJSONObject(oldResourceType);
                            properties.put(params.getPropertyName(), newValue);

                            if(null != obj){
                                JSONObject addObject = null;
                                if (obj.has("add")) {
                                    addObject = obj.getJSONObject("add");
                                }
                                JSONObject updateObject = null;
                                if (obj.has("update")) {
                                    updateObject = obj.getJSONObject("update");
                                }
                                JSONArray deleteArray = null;
                                if (obj.has("delete")) {
                                    deleteArray = obj.getJSONArray("delete");
                                }

                                if(null != addObject){
                                    handleAddProperties(addObject, properties, resource);
                                }
                                if(null != updateObject){
                                    handleUpdateProperties(updateObject, properties, resource);
                                }
                                if(null != deleteArray){
                                    handleDeleteProperties(deleteArray, properties, resource);
                                }
                            }
                        } catch (JSONException e) {
                            log.error("[ {} ] was not available in [ /content/dam/resource-props.json ]", oldResourceType, e);
                        }


                    } else {
                        log.error("Object is NULL due to component properties resource not found");
                    }
                    result.addSuccess(resource.getPath());
                    count++;
                } catch (Exception e) {
                    result.addFailure(resource.getPath());
                    log.warn("Could not update [ {}@" + params.getPropertyName() + " ]", resource.getPath(), e);
                }

                if (count == DEFAULT_BATCH_SIZE) {
                    this.save(resourceResolver, count);
                    count = 0;
                }
            }
        }


        this.save(resourceResolver, count);

        return result;
    }

    /**
     * Helper for saving changes to the JCR; contains timing logging.
     *
     * @param resourceResolver the resource resolver
     * @param size             the number of changes to save
     * @throws PersistenceException
     */
    private void save(final ResourceResolver resourceResolver, final int size) throws PersistenceException {
        if (resourceResolver.hasChanges()) {
            final long start = System.currentTimeMillis();
            resourceResolver.commit();
            if (log.isInfoEnabled()) {
                log.info("Imported a BATCH of [ {} ] assets in {} ms", size, System.currentTimeMillis() - start);
            }
        } else {
            log.debug("Nothing to save");
        }
    }

    private JSONObject getPropertiesMappingJsonFile(SlingHttpServletRequest request ){
        JSONObject jsonObj = null;
        try{
            Resource jsonResource = request.getResourceResolver().getResource("/content/dam/resource-props.json");
            if (null != jsonResource) {
                Asset asset = jsonResource.adaptTo(Asset.class);
                Resource original = asset.getOriginal();
                InputStream content = original.adaptTo(InputStream.class);
                StringBuilder sb = new StringBuilder();
                String line;
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        content, "UTF-8"));

                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
                jsonObj = new JSONObject(sb.toString());
            }else{
                log.error("Could not find file [ {} ]", "/content/dam/resource-props.json");
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObj;
    }

    private void handleAddProperties(JSONObject addFields, ModifiableValueMap properties, Resource resource) throws JSONException {
        Iterator<String> fields = addFields.keys();
        while (fields.hasNext()){
            String field = fields.next();
            try {
                if(!addFields.get(field).getClass().getName().equals("org.apache.sling.commons.json.JSONArray")){
                    properties.put(field, addFields.get(field));
                }else{
                    JSONArray jsonArray = addFields.getJSONArray(field);
                    JsonArray gJsonarray = new Gson().fromJson(jsonArray.toString(), JsonArray.class);
                    setArrayProperty(gJsonarray, field, resource);
                }
            } catch (Exception e) {
                log.warn("handleAddProperties : Could not add property [ {}@" + field + " : " + addFields.get(field) + " ]", resource.getPath(), e);
            }
        }

    }

    private void handleUpdateProperties(JSONObject updateFields, ModifiableValueMap properties, Resource resource) throws JSONException {
        Iterator<String> fields = updateFields.keys();
        while (fields.hasNext()){
            String field = fields.next();
            Object fieldVal = properties.get(field);
            try {
                if (properties.containsKey(field)) {
                    properties.remove(field);
                    properties.put(updateFields.getString(field), fieldVal);
                }
            } catch (Exception e) {
                log.warn("handleUpdateProperties : Could not update property [ {}@" + field + " : " + updateFields.get(field) + " ]", resource.getPath(), e);
            }
        }

    }

    private void handleDeleteProperties(JSONArray deleteFields, ModifiableValueMap properties, Resource resource) throws JSONException {
        for (int i = 0, size = deleteFields.length(); i < size; i++) {
            String field = deleteFields.getString(i);
            try {
                properties.remove(field);
            } catch (Exception e) {
                log.warn("handleDeleteProperties : Could not delete property [ {}@" + field +  " ]", resource.getPath(), e);
            }
        }
    }

    private void setArrayProperty(final JsonArray jsonArray, final String key, final Resource resource) {
        JsonPrimitive firstVal = jsonArray.get(0).getAsJsonPrimitive();

        try {
            Object[] values;
            if (firstVal.isBoolean()) {
                values = new Boolean[jsonArray.size()];
                for (int i = 0; i < jsonArray.size(); i++) {
                    values[i] = jsonArray.get(i).getAsBoolean();
                }
            } else if (DECIMAL_REGEX.matcher(firstVal.getAsString()).matches()) {
                values = new BigDecimal[jsonArray.size()];
                for (int i = 0; i < jsonArray.size(); i++) {
                    values[i] = jsonArray.get(i).getAsBigDecimal();
                }
            } else if (firstVal.isNumber()) {
                values = new Long[jsonArray.size()];
                for (int i = 0; i < jsonArray.size(); i++) {
                    values[i] = jsonArray.get(i).getAsLong();
                }
            } else {
                values = new String[jsonArray.size()];
                for (int i = 0; i < jsonArray.size(); i++) {
                    values[i] = jsonArray.get(i).getAsString();
                }
            }

            ValueMap resourceProperties = resource.adaptTo(ModifiableValueMap.class);
            resourceProperties.put(key, values);
            log.trace("Array property '{}' added for resource '{}'", key, resource.getPath());
        } catch (Exception e) {
            log.error("Unable to assign property '{}' to resource '{}'", key, resource.getPath());
        }
    }

    /**
     * Helper method; adds a message to the JSON Response object.
     *
     * @param jsonObject the JSON object to add the message to
     * @param message    the message to add.
     */
    private void addMessage(final JSONObject jsonObject, final String message) {
        try {
            jsonObject.put("message", message);
        } catch (JSONException e) {
            log.error("Could not formulate JSON Response", e);
        }
    }

    /**
     * Result to expose success and failure paths to the JSON response.
     */
    private class Result {
        private List<String> success;

        private List<String> failure;

        public Result() {
            success = new ArrayList<String>();
            failure = new ArrayList<String>();
        }

        public List<String> getSuccess() {
            return success;
        }

        public void addSuccess(String path) {
            this.success.add(path);
        }

        public List<String> getFailure() {
            return failure;
        }

        public void addFailure(String path) {
            this.failure.add(path);
        }
    }
}