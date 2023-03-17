package org.corpus_tools.pepperModules_SqueezerModule;

import java.util.*;
import java.util.stream.Collectors;

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
import org.corpus_tools.salt.core.*;
import org.corpus_tools.salt.core.SGraph.GRAPH_TRAVERSE_TYPE;
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
            Properties props = getProperties().getProperties();
            String targetLayer = props.getProperty(SqueezerModuleManipulatorProperties.TARGET_LAYER, null);
            // set up module properties
            List<SNode> nodes = new ArrayList<>(this.getDocument().getDocumentGraph().getNodes());

            // Loop over all SStructures and note the IDs of all the targets of their outgoing relations
            Map<Set<String>, List<SNode>> equalNodeMap = new HashMap<>();
            for (SNode n : nodes) {
                // Skip if not SStructure or if it's a signal node
                if (!(n instanceof SStructure) || n.getAnnotation("signal_type") != null) {
                    continue;
                }

                // Skip if not in target layer
                if (targetLayer != null) {
                    Set<String> layerNames = n.getLayers().stream()
                            .map(SNamedElement::getName)
                            .collect(Collectors.toSet());
                    if (!layerNames.contains(targetLayer)) {
                        continue;
                    }
                }

                // Note all TOKEN nodes that are direct "children" of this node
                List<SRelation> relations = n.getOutRelations();
                Set<String> targetSet = new HashSet<>();
                for (SRelation r : relations) {
                    if (r.getTarget() instanceof SToken) {
                        targetSet.add(r.getTarget().getId());
                    }
                }

                // Map: key is the set of direct children, value is a list of nodes with those children
                if (targetSet.isEmpty()) {
                    continue;
                }
                if (!equalNodeMap.containsKey(targetSet)) {
                    equalNodeMap.put(targetSet, new ArrayList<>());
                }
                equalNodeMap.get(targetSet).add(n);
            }

            Map<SNode, SNode> nodeMap = new HashMap<>();
            Set<SNode> toDelete = new HashSet<>();

            // For each set of nodes that have the exact same direct children...
            for (Map.Entry<Set<String>, List<SNode>> kvp : equalNodeMap.entrySet()) {
                List<SNode> sameNodes = kvp.getValue();

                // We only need to do something if there's more than one node with the same children
                if (sameNodes.size() >= 2) {

                    // Duplicated nodes that we want to squeeze out of the structure usually? have no annotation
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
                        if (!withAnno.isEmpty()) {
                            nodeMap.put(noAnno.get(i), withAnno.get(withAnno.size() - 1));
                        } else {
                            toDelete.add(noAnno.get(i));
                        }
                    }
                }
            }

            // Perform the deletion described above
            Collections.reverse(nodes);
            for (int i = nodes.size() - 1; i >= 0; i--) {
                SNode n = nodes.get(i);
                if (nodeMap.containsKey(n)) {
                    SNode n2 = nodeMap.get(n);
                    for (SRelation r : n.getInRelations()) {
                        r.setTarget(n2);
                    }
                    for (SRelation r : n.getOutRelations()) {
                        if (r instanceof SPointingRelation) {
                            r.setSource(n2);
                        } else {
                            this.getDocument().getDocumentGraph().removeRelation(r);
                        }
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
