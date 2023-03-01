package org.corpus_tools.pepperModules_SqueezerModule;

import java.util.*;

import org.corpus_tools.pepper.common.DOCUMENT_STATUS;
import org.corpus_tools.pepper.common.PepperConfiguration;
import org.corpus_tools.pepper.impl.PepperManipulatorImpl;
import org.corpus_tools.pepper.impl.PepperMapperImpl;
import org.corpus_tools.pepper.modules.PepperManipulator;
import org.corpus_tools.pepper.modules.PepperMapper;
import org.corpus_tools.pepper.modules.PepperModule;
import org.corpus_tools.pepper.modules.PepperModuleProperties;
import org.corpus_tools.pepper.modules.exceptions.PepperModuleDataException;
import org.corpus_tools.pepper.modules.exceptions.PepperModuleException;
import org.corpus_tools.pepper.modules.exceptions.PepperModuleNotReadyException;
import org.corpus_tools.salt.SALT_TYPE;
import org.corpus_tools.salt.SaltFactory;
import org.corpus_tools.salt.common.*;
import org.corpus_tools.salt.core.GraphTraverseHandler;
import org.corpus_tools.salt.core.SAnnotation;
import org.corpus_tools.salt.core.SGraph.GRAPH_TRAVERSE_TYPE;
import org.corpus_tools.salt.core.SLayer;
import org.corpus_tools.salt.core.SNode;
import org.corpus_tools.salt.core.SRelation;
import org.corpus_tools.salt.graph.Identifier;
import org.corpus_tools.salt.graph.Node;
import org.eclipse.emf.common.util.URI;
import org.osgi.service.component.annotations.Component;

/**
 * This is a {@link PepperManipulator} which can move annotations, for 
 * example from edges to their attached source or target nodes.
 *
 * @author Luke Gessler
 */
@Component(name = "SqueezerModuleManipulatorComponent", factory = "PepperManipulatorComponentFactory")
public class SqueezerModuleManipulator extends PepperManipulatorImpl {

    public SqueezerModuleManipulator() {
        super();
        setName("Squeezer");
        setSupplierContact(URI.createURI(PepperConfiguration.EMAIL));
        setSupplierHomepage(URI.createURI(PepperConfiguration.HOMEPAGE));
        setDesc("This manipulator deletes nodes with no annotations.");
    }

    /**
     * @param Identifier
     *            {@link Identifier} of the {@link SCorpus} or {@link SDocument}
     *            to be processed.
     * @return {@link PepperMapper} object to do the mapping task for object
     *         connected to given {@link Identifier}
     */
    public PepperMapper createPepperMapper(Identifier Identifier) {
        SqueezerModuleMapper mapper = new SqueezerModuleMapper();
        return (mapper);
    }


    public static class SqueezerModuleMapper extends PepperMapperImpl implements GraphTraverseHandler {

        @Override
        public DOCUMENT_STATUS mapSCorpus() {
            return (DOCUMENT_STATUS.COMPLETED);
        }

        @Override
        public DOCUMENT_STATUS mapSDocument() {
            String targetLayer = getProperties().getProperties().getProperty(SqueezerModuleManipulatorProperties.TARGET_LAYER, null);
            // set up module properties
            List<SNode> nodes = this.getDocument().getDocumentGraph().getNodes();

            // Loop over all SStructures and note the IDs of all the targets of their outgoing relations
            Map<Set<String>, List<SNode>> equalNodeMap = new HashMap<>();
            for (SNode n : nodes) {
                // Skip if not SStructure
                if (!(n instanceof SStructure)) {
                    continue;
                }
                // Skip if not in target layer
                Set<String> layerNames = new HashSet<>();
                for (SLayer l : n.getLayers()) {
                    layerNames.add(l.getName());
                }
                if (targetLayer != null && !layerNames.contains(targetLayer)) {
                    continue;
                }
                List<SRelation> relations = n.getOutRelations();
                Set<String> targetSet = new HashSet<>();
                for (SRelation r : relations) {
                    targetSet.add(r.getTarget().getId());
                }
                if (!equalNodeMap.containsKey(targetSet)) {
                    equalNodeMap.put(targetSet, new ArrayList<SNode>());
                }
                equalNodeMap.get(targetSet).add(n);
            }

            // Find SStructures that overlap exactly in their outgoing target ID sets.
            // If we find two nodes that overlap exactly, then assume that one of them
            // does not have annotations. Move the incoming relation for the annotation-less
            // node to the node with annotations, and then delete the annotation-less node.
            Map<SNode, SNode> nodeMap = new HashMap<>();
            Set<SNode> toDelete = new HashSet<>();
            for (Map.Entry<Set<String>, List<SNode>> kvp : equalNodeMap.entrySet()) {
                List<SNode> sameNodes = kvp.getValue();
                if (sameNodes.size() >= 2) {
                    List<SNode> noAnno = new ArrayList<>();
                    List<SNode> withAnno = new ArrayList<>();
                    for (SNode n : sameNodes) {
                        if (n.getAnnotations().size() == 0) {
                            noAnno.add(n);
                        } else {
                            withAnno.add(n);
                        }
                    }

                    if (noAnno.size() < withAnno.size()) {
                        throw new PepperModuleException("Expected at least as many noAnno nodes as withAnno nodes");
                    }

                    int i;
                    for (i = 0; i < withAnno.size(); i++) {
                        nodeMap.put(noAnno.get(i), withAnno.get(i));
                    }
                    for ( ; i < noAnno.size(); i++) {
                        toDelete.add(noAnno.get(i));
                    }
                }
            }

            // Perform the deletion described above
            for (int i = nodes.size() - 1; i >= 0; i--) {
                SNode n = nodes.get(i);
                if (nodeMap.containsKey(n)) {
                    SNode n2 = nodeMap.get(n);
                    for (SRelation r : n.getInRelations()) {
                        r.setTarget(n2);
                    }
                    for (SRelation r : n.getOutRelations()) {
                        this.getDocument().getDocumentGraph().removeRelation(r);
                    }
                    this.getDocument().getDocumentGraph().removeNode(n);
                }
                if (toDelete.contains(n)) {
                    this.getDocument().getDocumentGraph().removeNode(n);
                }
            }

            return (DOCUMENT_STATUS.COMPLETED);
        }

        @Override
        public void nodeReached(GRAPH_TRAVERSE_TYPE traversalType, String traversalId, SNode currNode, SRelation sRelation, SNode fromNode, long order) {

        }

        @Override
        public void nodeLeft(GRAPH_TRAVERSE_TYPE traversalType, String traversalId, SNode currNode, SRelation edge, SNode fromNode, long order) {
        }

        @Override
        public boolean checkConstraint(GRAPH_TRAVERSE_TYPE traversalType, String traversalId, SRelation edge, SNode currNode, long order) {
            if (currNode instanceof STextualDS) {
                return (false);
            } else {
                return (true);
            }
        }
    }

    @Override
    public boolean isReadyToStart() throws PepperModuleNotReadyException {
        return true;
    }



}
