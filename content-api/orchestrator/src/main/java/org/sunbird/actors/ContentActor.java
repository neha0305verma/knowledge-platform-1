package org.sunbird.actors;

import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.cache.impl.RedisCache;
import org.sunbird.common.ContentParams;
import org.sunbird.common.Platform;
import org.sunbird.common.dto.Request;
import org.sunbird.common.dto.Response;
import org.sunbird.common.dto.ResponseHandler;
import org.sunbird.common.exception.ClientException;
import org.sunbird.common.exception.ServerException;
import org.sunbird.graph.dac.model.Node;
import org.sunbird.graph.nodes.DataNode;
import org.sunbird.graph.utils.NodeUtil;
import org.sunbird.managers.HierarchyManager;
import org.sunbird.utils.RequestUtils;
import scala.concurrent.Future;
import org.sunbird.utils.CopyOperation;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

public class ContentActor extends BaseActor {

    public Future<Response> onReceive(Request request) throws Throwable {
        String operation = request.getOperation();
        switch(operation) {
            case "createContent": return create(request);
            case "readContent": return read(request);
            case "updateContent": return update(request);
            case "copy": return copy(request);
            default: return ERROR(operation);
        }
    }

    private Future<Response> create(Request request) throws Exception {
        populateDefaultersForCreation(request);
        RequestUtils.restrictProperties(request);
        return DataNode.create(request, getContext().dispatcher())
                .map(new Mapper<Node, Response>() {
                    @Override
                    public Response apply(Node node) {
                        Response response = ResponseHandler.OK();
                        response.put("node_id", node.getIdentifier());
                        response.put("identifier", node.getIdentifier());
                        response.put("versionKey", node.getMetadata().get("versionKey"));
                        return response;
                    }
                }, getContext().dispatcher());
    }

    private Future<Response> update(Request request) throws Exception {
        populateDefaultersForUpdation(request);
        if(StringUtils.isBlank((String)request.get("versionKey")))
            throw new ClientException("ERR_INVALID_REQUEST", "Please Provide Version Key!");
        RequestUtils.restrictProperties(request);
        return DataNode.update(request, getContext().dispatcher())
                .map(new Mapper<Node, Response>() {
                    @Override
                    public Response apply(Node node) {
                        Response response = ResponseHandler.OK();
                        String identifier = node.getIdentifier().replace(".img","");
                        response.put("node_id", identifier);
                        response.put("identifier", identifier);
                        response.put("versionKey", node.getMetadata().get("versionKey"));
                        return response;
                    }
                }, getContext().dispatcher());
    }

    private Future<Response> read(Request request) throws Exception {
        List<String> fields = Arrays.stream(((String) request.get("fields")).split(","))
                .filter(field -> StringUtils.isNotBlank(field) && !StringUtils.equalsIgnoreCase(field, "null")).collect(Collectors.toList());
        request.getRequest().put("fields", fields);
        return DataNode.read(request, getContext().dispatcher())
                .map(new Mapper<Node, Response>() {
                    @Override
                    public Response apply(Node node) {
                        Map<String, Object> metadata = NodeUtil.serialize(node, fields, (String) request.getContext().get("schemaName"), (String)request.getContext().get("version"));
                        Response response = ResponseHandler.OK();
                        response.put("content", metadata);
                        return response;
                    }
                }, getContext().dispatcher());
    }

    private Future<Response> copy(Request request) throws Exception {
        RequestUtils.restrictProperties(request);
            List<String> externalPropList = Platform.config.hasPath("learning.content.copy.external_prop_list")
                    ? Platform.config.getStringList("learning.content.copy.external_prop_list") : null;
            request.getRequest().put("fields", externalPropList);
        return DataNode.read(request, getContext().dispatcher()).map(new Mapper<Node, Response>() {
            @Override
            public Response apply(Node existingNode) {
                Response response = ResponseHandler.OK();
                try {
                    Map<String, String> idMap = prepareCopyNode(request, existingNode);
                    response.put("node_id", idMap);
                } catch (Exception e){
                    throw new ServerException("ERR_CREATE_COPY_NODE","Error while creating copy node");
                }
                return response;
              }
             }, getContext().dispatcher());
    }

    private static void populateDefaultersForCreation(Request request) {
        setDefaultsBasedOnMimeType(request, ContentParams.create.name());
        setDefaultLicense(request);
    }

    private static void populateDefaultersForUpdation(Request request){
        if(request.getRequest().containsKey(ContentParams.body.name()))
            request.put(ContentParams.artifactUrl.name(), null);
    }

    private static void setDefaultLicense(Request request) {
        if(StringUtils.isEmpty((String)request.getRequest().get("license"))){
            String cacheKey = "channel_" + (String) request.getRequest().get("channel") + "_license";
	        String defaultLicense = RedisCache.get(cacheKey, null, 0);
            if(StringUtils.isNotEmpty(defaultLicense))
                request.getRequest().put("license", defaultLicense);
            else
                System.out.println("Default License is not available for channel: " + (String)request.getRequest().get("channel"));
        }
    }

    private static void setDefaultsBasedOnMimeType(Request request, String operation) {

        String mimeType = (String) request.get(ContentParams.mimeType.name());
        if (StringUtils.isNotBlank(mimeType) && operation.equalsIgnoreCase(ContentParams.create.name())) {
            if (StringUtils.equalsIgnoreCase("application/vnd.ekstep.plugin-archive", mimeType)) {
                String code = (String) request.get(ContentParams.code.name());
                if (null == code || StringUtils.isBlank(code))
                    throw new ClientException("ERR_PLUGIN_CODE_REQUIRED", "Unique code is mandatory for plugins");
                request.put(ContentParams.identifier.name(), request.get(ContentParams.code.name()));
            } else {
                request.put(ContentParams.osId.name(), "org.ekstep.quiz.app");
            }

            if (mimeType.endsWith("archive") || mimeType.endsWith("vnd.ekstep.content-collection")
                    || mimeType.endsWith("epub"))
                request.put(ContentParams.contentEncoding.name(), ContentParams.gzip.name());
            else
                request.put(ContentParams.contentEncoding.name(), ContentParams.identity.name());

            if (mimeType.endsWith("youtube") || mimeType.endsWith("x-url"))
                request.put(ContentParams.contentDisposition.name(), ContentParams.online.name());
            else
                request.put(ContentParams.contentDisposition.name(), ContentParams.inline.name());
        }
    }

    private Map<String, String> prepareCopyNode(Request request, Node existingNode) throws Exception{
        Node validatedExistingNode = CopyOperation.validateCopyContentRequest(existingNode, (Map<String, Object>) request.getRequest(), (String) request.getRequest().get("mode"));
        existingNode.setGraphId((String) request.getContext().get("graph_id"));
        Node copyNode = CopyOperation.copy(validatedExistingNode, (Map<String, Object>) request.getRequest(), (String) request.getRequest().get("mode"));
        request.getRequest().clear();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> copiedMap = mapper.convertValue(copyNode.getMetadata(), new TypeReference<Map<String,Object>>(){});
        request.setRequest(copiedMap);
        Future<Node> copiedNode = null;
        try {
            copiedNode = createCopyNode(request, validatedExistingNode);
        } catch (Exception e){
           throw new ServerException("ERR_CREATE_COPY_NODE","Error while creating copy node");
        }
        Map<String,String> idMap = new HashMap<>();
        idMap.put(existingNode.getIdentifier(), copyNode.getIdentifier());
        return idMap;
    }

    private Future<Node> createCopyNode(Request request, Node existingNode) throws Exception{
        Future<Node> copiedNode =  DataNode.create(request, getContext().dispatcher()).map(new Mapper<Node, Node>() {
            @Override
            public Node apply(Node node) {
                CopyOperation.uploadArtifactUrl(existingNode, node);
                return node;
            }
        }, getContext().dispatcher());
        copiedNode.onComplete(new OnComplete<Node>(){
            public void onComplete(Throwable t, Node result) throws Exception{
                if(equalsIgnoreCase((String) existingNode.getMetadata().get("mimeType"), "application/vnd.ekstep.content-collection")){
                    // Generating update hierarchy with copied parent content and calling
                    // update hierarchy.
                    Future<Response> response = HierarchyManager.getHierarchy(request, getContext().dispatcher());
                    response.onComplete(new OnComplete<Response>() {
                        public void onComplete(Throwable t, Response response) {
                            CopyOperation.prepareHierarchy(response);
                        }
                    }, getContext().dispatcher());
                }
            }
        }, getContext().dispatcher());
        return copiedNode;
    }

}
