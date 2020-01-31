package org.sunbird.utils;

import akka.dispatch.Mapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.Platform;
import org.sunbird.common.dto.Request;
import org.sunbird.common.dto.Response;
import org.sunbird.common.dto.ResponseHandler;
import org.sunbird.common.exception.ClientException;
import org.sunbird.common.exception.ServerException;
import org.sunbird.graph.common.Identifier;
import org.sunbird.graph.dac.model.Node;
import org.sunbird.graph.dac.model.Relation;
import org.sunbird.graph.external.ExternalPropsManager;
import org.sunbird.graph.nodes.DataNode;
import org.sunbird.managers.HierarchyManager;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

public class CopyOperation {

    public static Node prepareCopyNode(Node existingNode, Map<String, Object> requestMap, String mode) {
        String newId = Identifier.getIdentifier(existingNode.getGraphId(), Identifier.getUniqueIdFromTimestamp());
        Node copyNode = new Node(newId, existingNode.getNodeType(), existingNode.getObjectType());

        Map<String, Object> metaData = new HashMap<>();
        metaData.remove("identifier");
        metaData.putAll(existingNode.getMetadata());

        Map<String, Object> originData = new HashMap<>();

        List<String> originNodeMetadataList = Platform.config.hasPath("learning.content.copy.origin_data")
                ? Platform.config.getStringList("learning.content.copy.origin_data") : null;
        if(CollectionUtils.isNotEmpty(originNodeMetadataList))
            originNodeMetadataList.forEach(meta -> {
                if(metaData.containsKey(meta))
                    originData.put(meta, metaData.get(meta));
            });

        List<String> nullPropList = Platform.config.hasPath("learning.content.copy.props_to_remove")
                ? Platform.config.getStringList("learning.content.copy.props_to_remove"): null;

        if(CollectionUtils.isNotEmpty(nullPropList))
            nullPropList.forEach(prop -> metaData.remove(prop));

        copyNode.setMetadata(metaData);
        copyNode.setGraphId(existingNode.getGraphId());
        requestMap.remove("mode");
        copyNode.getMetadata().putAll(requestMap);
        copyNode.getMetadata().put("status", "Draft");
        copyNode.getMetadata().put("origin", existingNode.getIdentifier());
        copyNode.getMetadata().put("identifier",newId);
        // remove after imimetype implemented
        copyNode.getMetadata().put("artifactUrl",existingNode.getMetadata().get("artifactUrl"));
        if(MapUtils.isNotEmpty(originData))
            copyNode.getMetadata().put("originData", originData);

        List<Relation> existingNodeOutRelations = existingNode.getOutRelations();
        List<Relation> copiedNodeOutRelations = new ArrayList<>();
        if (!CollectionUtils.isEmpty(existingNodeOutRelations)) {
            for (Relation rel : existingNodeOutRelations) {
                if (!Arrays.asList("Content", "ContentImage").contains(rel.getEndNodeObjectType())) {
                    copiedNodeOutRelations.add(new Relation(newId, rel.getRelationType(), rel.getEndNodeId()));
                }
            }
        }
        copyNode.setOutRelations(copiedNodeOutRelations);
        return copyNode;
    }

    public static Node validateCopyContentRequest(Node existingNode, Map<String, Object> requestMap, String mode) {
        if (null == requestMap)
            throw new ClientException("ERR_INVALID_REQUEST", "Please provide valid request");

        validateOrThrowExceptionForEmptyKeys(requestMap, "Content", Arrays.asList("createdBy", "createdFor",
                "organisation", "framework"));

        List<String> notCoppiedContent = null;
        if (CollectionUtils.isEmpty(Platform.config.getStringList("learning.content.type.not.copied.list"))) {
            notCoppiedContent = Platform.config.getStringList("learning.content.type.not.copied.list");
        }
        if (!CollectionUtils.isEmpty(notCoppiedContent) && notCoppiedContent.contains(existingNode.getMetadata().get("contentType"))) {
            throw new ClientException("CONTENTTYPE_ASSET_CAN_NOT_COPY",
                    "ContentType " + existingNode.getMetadata().get("contentType") + " can not be copied.");
        }
        String status = (String) existingNode.getMetadata().get("status");
        List<String> invalidStatusList = Platform.config.getStringList("learning.content.copy.invalid_status_list");
        if (invalidStatusList.contains(status))
            throw new ClientException("ERR_INVALID_REQUEST",
                    "Cannot copy content in " + status.toLowerCase() + " status");

        return existingNode;
    }

    private static boolean validateOrThrowExceptionForEmptyKeys(Map<String, Object> requestMap, String prefix, List<String> keys) {
        String errMsg = "Please provide valid value for ";
        boolean flag = false;
        List<String> notFoundKeys = null;
        for (String key : keys) {
            if (null == requestMap.get(key)) {
                flag = true;
            } else if (requestMap.get(key) instanceof Map) {
                flag = MapUtils.isEmpty((Map) requestMap.get(key));
            } else if (requestMap.get(key) instanceof List) {
                flag = CollectionUtils.isEmpty((List) requestMap.get(key));
            } else {
                flag = isBlank((String) requestMap.get(key));
            }
            if (flag) {
                if(null==notFoundKeys)
                    notFoundKeys = new ArrayList<>();
                notFoundKeys.add(key);
            }
        }
        if (CollectionUtils.isEmpty(notFoundKeys))
            return true;
        else {
            errMsg = errMsg + String.join(", ", notFoundKeys) + ".";
        }
        throw new ClientException("ERR_INVALID_REQUEST", errMsg.trim().substring(0, errMsg
                .length()-1));
    }

    public static void uploadArtifactUrl(Node existingNode, Node copyNode) {
        System.out.println("uploadArtifactUrl");
        File file =null;
        try {
            String artifactUrl = (String) existingNode.getMetadata().get("artifactUrl");
            if (StringUtils.isNotBlank(artifactUrl)) {
                Response response = null;
                String mimeType = (String) copyNode.getMetadata().get("mimeType");
                String contentType = (String) copyNode.getMetadata().get("contentType");

//            if (!(StringUtils.equalsIgnoreCase("application/vnd.ekstep.ecml-archive", mimeType)
//                    || StringUtils.equalsIgnoreCase("application/vnd.ekstep.content-collection", mimeType))) {
//                    IMimeTypeManager mimeTypeManager = MimeTypeManagerFactory.getManager(contentType, mimeType);
//                    BaseMimeTypeManager baseMimeTypeManager = new BaseMimeTypeManager();
//
//                    if (baseMimeTypeManager.isS3Url(artifactUrl)) {
//                        file = copyURLToFile(artifactUrl);
//                        if (isH5PMimeType(mimeType)) {
//                            H5PMimeTypeMgrImpl h5pManager = new H5PMimeTypeMgrImpl();
//                            response = h5pManager.upload(copyNode.getIdentifier(), copyNode, true, file);
//                        } else {
//                            response = mimeTypeManager.upload(copyNode.getIdentifier(), copyNode, file, false);
//                        }
//
//                    } else {
//                        response = mimeTypeManager.upload(copyNode.getIdentifier(), copyNode, artifactUrl);
//                    }
//
//                    if (null == response || checkError(response)) {
//                        throw new ClientException("ARTIFACT_NOT_COPIED", "ArtifactUrl not coppied.");
//                    }
//                }

            }
        } finally {
            if(null != file && file.exists())
                file.delete();
        }
    }

    protected static File copyURLToFile(String fileUrl) {
        try {
            String fileName = getFileNameFromURL(fileUrl);
            File file = new File(fileName);
            FileUtils.copyURLToFile(new URL(fileUrl), file);
            return file;
        } catch (IOException e) {
            throw new ClientException("ERR_INVALID_UPLOAD_FILE_URL", "fileUrl is invalid.");
        }
    }

    protected static String getFileNameFromURL(String fileUrl) {
        String fileName = FilenameUtils.getBaseName(fileUrl) + "_" + System.currentTimeMillis();
        if (!FilenameUtils.getExtension(fileUrl).isEmpty())
            fileName += "." + FilenameUtils.getExtension(fileUrl);
        return fileName;
    }

    public static void prepareUpdateHierarchy(Response response) {
        if(!ResponseHandler.checkError(response)) {
            if(MapUtils.isEmpty(response.getResult()))
                throw new ServerException("ERR_WHILE_UPDATING_HIERARCHY", "Error while reading hierarchy for copycontent");
        }
        Map<String, Object> contentMap = (Map<String, Object>) response.getResult().get("content");

//        Map<String, Object> updateRequest = prepareUpdateHierarchyRequest(
//                (List<Map<String, Object>>) contentMap.get("children"), existingNode, idMap);

        Map<String, Object> nodesModified = new HashMap<>();
        Map<String, Object> hierarchy = new HashMap<>();

        Map<String, Object> parentHierarchy = new HashMap<>();
        parentHierarchy.put("children", new ArrayList<>());
        parentHierarchy.put("root", true);
        parentHierarchy.put("contentType", ((Map<String, Object>) response.getResult().get("content")).get("contentType"));
        hierarchy.put((String) (((Map<String, Object>) response.getResult().get("content")).get("identifier")), parentHierarchy);
        populateHierarchy((List<Map<String, Object>>) ((Map<String, Object>) response.getResult().get("content")).get("children"), nodesModified, hierarchy, (String) (((Map<String, Object>) response.getResult().get("content")).get("identifier")));

        Map<String, Object> data = new HashMap<>();
        data.put("nodesModified", nodesModified);
        data.put("hierarchy", hierarchy);

        return data;

//        Response response = this.hierarchyManager.update(updateRequest);
//        if (checkError(response)) {
//            TelemetryManager.error("CopyContent: Error while updating hierarchy: " + response.getParams().getErr() + " :: " + response.getParams().getErrmsg() + response.getResult());
//            if(MapUtils.isNotEmpty(response.getResult()) && graphValidationErrors.contains(response.getParams().getErr()))
//                throw new ServerException(readResponse.getParams().getErr(), readResponse.getResult().toString());
//            else
//                throw new ServerException(readResponse.getParams().getErr(), readResponse.getParams().getErrmsg());
//        }
    }

    private static void populateHierarchy(List<Map<String, Object>> children, Map<String, Object> nodesModified,
                                   Map<String, Object> hierarchy, String parentId) {
        List<String> nullPropList = Platform.config.getStringList("learning.content.copy.props_to_remove");
        if (null != children && !children.isEmpty()) {
            for (Map<String, Object> child : children) {
                String id = (String) child.get("identifier");
                if (equalsIgnoreCase("Parent", (String) child.get("visibility"))) {
                    // NodesModified and hierarchy
                    id = UUID.randomUUID().toString();
                    Map<String, Object> metadata = new HashMap<>();
                    metadata.putAll(child);
                    nullPropList.forEach(prop -> metadata.remove(prop));
                    metadata.put("children", new ArrayList<>());
                    metadata.remove("identifier");
                    metadata.remove("parent");
                    metadata.remove("index");
                    metadata.remove("depth");

                    // TBD: Populate artifactUrl

                    Map<String, Object> modifiedNode = new HashMap<>();
                    modifiedNode.put("metadata", metadata);
                    modifiedNode.put("root", false);
                    modifiedNode.put("isNew", true);
                    nodesModified.put(id, modifiedNode);
                }
                Map<String, Object> parentHierarchy = new HashMap<>();
                parentHierarchy.put("children", new ArrayList<>());
                parentHierarchy.put("root", false);
                parentHierarchy.put("contentType", child.get("contentType"));
                hierarchy.put(id, parentHierarchy);
                ((List) ((Map<String, Object>) hierarchy.get(parentId)).get("children")).add(id);

                populateHierarchy((List<Map<String, Object>>) child.get("children"), nodesModified, hierarchy, id);
            }
        }
    }
}
