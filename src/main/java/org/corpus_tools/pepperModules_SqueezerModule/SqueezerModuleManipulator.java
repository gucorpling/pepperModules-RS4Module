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
import org.corpus_tools.salt.graph.Relation;
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

        private Map<UUID, SToken> tokenMap;
        private Map<UUID, SStructure> structureMap;
        private SLayer layer;
        private Map<SStructure, List<SStructure>> signalsForSecondaryEdge = null;
        private Map<String, SStructure> secondaryEdgeIndex = null;
        private Set<SDominanceRelation> primaryRelationsWithSignals = null;

        public SqueezerModuleMapper() {
            tokenMap = new HashMap<>();
            structureMap = new HashMap<>();
            signalsForSecondaryEdge = new HashMap<>();
            secondaryEdgeIndex = new HashMap<>();
            primaryRelationsWithSignals = new HashSet<>();
        }

        @Override
        public DOCUMENT_STATUS mapSCorpus() {
            return (DOCUMENT_STATUS.COMPLETED);
        }

        private void indexUuids() {
            for (SToken t : getDocument().getDocumentGraph().getTokens()) {
                SAnnotation ann = t.getAnnotation("TEMP", "uuid");
                if (ann != null) {
                    tokenMap.put((UUID) ann.getValue(), t);
                    t.removeLabel("TEMP", "uuid");
                }
            }
            for (SNode n : getDocument().getDocumentGraph().getNodes()) {
                SAnnotation ann = n.getAnnotation("TEMP", "uuid");
                if (ann != null) {
                    structureMap.put((UUID) ann.getValue(), (SStructure) n);
                    n.removeLabel("TEMP", "uuid");
                }
            }
        }

        private void constructSecEdges() {
            for (SNode n : this.structureMap.values()) {
                SAnnotation secedgeAnn = n.getAnnotation("TEMP", "secedges");
                if (secedgeAnn == null) {
                    continue;
                }
                for (Map<String, Object> secedgeMap : ((List<Map<String, Object>>) secedgeAnn.getValue())) {
                    UUID sourceId = (UUID) secedgeMap.get("edgeSource");
                    UUID targetId = (UUID) secedgeMap.get("edgeTarget");
                    SStructure source = this.structureMap.get(sourceId);
                    SStructure target = this.structureMap.get(targetId);
                    SStructure ses = SaltFactory.createSStructure();
                    ses.createAnnotation("sec", "relname",secedgeMap.get("relationName"));

                    SDominanceRelation sourceRel = SaltFactory.createSDominanceRelation();
                    sourceRel.setSource(ses);
                    sourceRel.setTarget(source);
                    sourceRel.createAnnotation(null, "end", "source");

                    SDominanceRelation targetRel = SaltFactory.createSDominanceRelation();
                    targetRel.setSource(ses);
                    targetRel.setTarget(target);
                    targetRel.createAnnotation(null, "end", "target");

                    getDocument().getDocumentGraph().addNode(ses);
                    getDocument().getDocumentGraph().addRelation(sourceRel);
                    getDocument().getDocumentGraph().addRelation(targetRel);
                    if (layer != null) {
                        layer.addNode(ses);
                        layer.addRelation(sourceRel);
                        layer.addRelation(targetRel);
                    }
                    secondaryEdgeIndex.put(sourceId + "-" + targetId, ses);
                }
                n.removeLabel("TEMP", "secedges");
            }
        }

        private void constructSignal(SNode source, Map<Object, Object> signal) {
            String signalType = (String) signal.get("signal:type");
            String signalSubtype = (String) signal.get("signal:subtype");
            List<Integer> tokenIds = (List<Integer>) signal.get("signal:tokens");
            List<UUID> sourceIds = (List<UUID>) signal.get("signal:source");
            boolean isSecondary = sourceIds.size() > 1;

            // Create the signal node
            SStructure signalNode = SaltFactory.createSStructure();
            signalNode.createAnnotation(null, "signal_type", signalType);
            signalNode.createAnnotation(null, "signal_subtype", signalSubtype);
            this.getDocument().getDocumentGraph().addNode(signalNode);
            if (layer != null) {
                layer.addNode(signalNode);
            }

            // Signal node to tokens
            // add annotations to the signal node: signal_text for space-separated tokens, signal_indexes for their indexes
            List<SToken> tokens = this.getDocument().getDocumentGraph().getTokens();
            if (tokenIds != null) {
                StringBuilder tokenTextSb = new StringBuilder();
                StringBuilder tokenIndexesSb = new StringBuilder();
                for (int i = 0; i < tokenIds.size(); i++) {
                    SToken token = tokens.get(tokenIds.get(i) - 1);
                    String tokenText = getDocument().getDocumentGraph().getText(token);

                    tokenTextSb.append(tokenText);
                    tokenIndexesSb.append(tokenIds.get(i).toString());
                    if (i < tokenIds.size() - 1) {
                        tokenTextSb.append(" ");
                        tokenIndexesSb.append(" ");
                    }
                }
                signalNode.createAnnotation(null, "signal_text", tokenTextSb.toString());
                signalNode.createAnnotation(null, "signal_indexes", tokenIndexesSb.toString());
            }

            // also make the signal node dominate every token
            int earliestToken = Integer.MAX_VALUE;
            if (tokenIds != null) {
                List<SToken> sTokens = this.getDocument().getDocumentGraph().getTokens();
                for (int tokenId : tokenIds) {
                    SDominanceRelation tokRel = SaltFactory.createSDominanceRelation();
                    tokRel.setSource(signalNode);
                    // tokens are 1-indexed, list is 0-indexed
                    tokRel.setTarget(sTokens.get(tokenId - 1));
                    tokRel.setType("signal_token");
                    tokRel.setSource(signalNode);
                    this.getDocument().getDocumentGraph().addRelation(tokRel);
                    if (layer != null) {
                        layer.addRelation(tokRel);
                    }
                    if (tokenId < earliestToken) {
                        earliestToken = tokenId;
                    }
                }
            }

            // determine relname
            String relname = null;
            List<SRelation> incomingRelations = source.getInRelations();
            if (incomingRelations != null) {
                for (SRelation r : incomingRelations) {
                    if (!isSecondary) {
                        if (r instanceof SDominanceRelation) {
                            SAnnotation ann = r.getAnnotation(null, "relname");
                            this.primaryRelationsWithSignals.add((SDominanceRelation) r);
                            if (ann != null) {
                                relname = (String) ann.getValue();
                            }
                        }
                    } else {
                        if (r instanceof SDominanceRelation
                                && r.getAnnotation(null, "end") != null
                                && r.getAnnotation(null, "end").getValue().equals("source")) {
                            SStructure s = (SStructure) r.getSource();
                            UUID targetId = sourceIds.get(1);
                            for (SRelation r2 : s.getOutRelations()) {
                                if (r2 instanceof SDominanceRelation
                                        && this.structureMap.get(targetId).equals(r2.getTarget())) {
                                    relname = (String) (s.getAnnotation("sec", "relname").getValue());
                                }
                            }
                        }
                    }
                }
            }

            // Signal node to other RST nodes
            if (isSecondary) {
                // If we have a secondary edge associated with the signal, then our strategy is going to be
                // different: the signal node will have two dominance relations, one for each of the SE's ends
                SStructure seSource = this.structureMap.get(sourceIds.get(0));
                SStructure seTarget = this.structureMap.get(sourceIds.get(1));

                SDominanceRelation signal2source = SaltFactory.createSDominanceRelation();
                SDominanceRelation signal2target = SaltFactory.createSDominanceRelation();
                signal2source.setSource(signalNode);
                signal2source.setTarget(seSource);
                signal2target.setSource(signalNode);
                signal2target.setTarget(seTarget);

                if (relname != null) {
                    signal2source.createAnnotation("sec", "signal", relname);
                    signalNode.createAnnotation("sec", "signaled_relation", relname);
                }
                this.getDocument().getDocumentGraph().addRelation(signal2source);
                this.getDocument().getDocumentGraph().addRelation(signal2target);
                if (layer != null) {
                    layer.addRelation(signal2source);
                    layer.addRelation(signal2target);
                }
                signalNode.createProcessingAnnotation(null, "earliest_token", earliestToken);
                SStructure secondaryEdge = this.secondaryEdgeIndex.get(sourceIds.get(0) + "-" + sourceIds.get(1));
                if (!this.signalsForSecondaryEdge.containsKey(secondaryEdge)) {
                    this.signalsForSecondaryEdge.put(secondaryEdge, new ArrayList<>());
                }
                this.signalsForSecondaryEdge.get(secondaryEdge).add(signalNode);
            } else {
                SDominanceRelation signal2rstNode = SaltFactory.createSDominanceRelation();
                signal2rstNode.setSource(signalNode);
                signal2rstNode.setTarget((SStructuredNode) source);

                if (relname != null) {
                    signal2rstNode.createAnnotation("prim", "signal", relname);
                    signalNode.createAnnotation("prim", "signaled_relation", relname);
                }

                this.getDocument().getDocumentGraph().addRelation(signal2rstNode);
                if (layer != null) {
                    layer.addNode(signalNode);
                }
            }
        }

        private void constructSignals() {
            for (Map.Entry<UUID, SStructure> kvp : this.structureMap.entrySet()) {
                SNode n = kvp.getValue();
                SAnnotation ann = n.getAnnotation("TEMP", "signals");
                if (ann != null) {
                    for (Map<Object, Object> signal : ((List<Map<Object, Object>>) ann.getValue())) {
                        constructSignal(n, signal);
                    }
                    n.removeLabel("TEMP", "signals");
                }
            }
        }

        private void connectSecondaryEdgesToSignals() {
            for (Map.Entry<SStructure, List<SStructure>> kvp : this.signalsForSecondaryEdge.entrySet()) {
                SStructure secEdge = kvp.getKey();
                List<SStructure> signals = kvp.getValue();

                SStructure winningSignal = null;
                int earliestToken = Integer.MAX_VALUE;
                for (SStructure signal : signals) {
                    int signalEarliestToken = (Integer) signal.getProcessingAnnotation("earliest_token").getValue();
                    if (signalEarliestToken < earliestToken) {
                        earliestToken = signalEarliestToken;
                        winningSignal = signal;
                    }
                }
                // If we don't have any signal, we should really complain, but just be lenient and allow secedges
                // without any signals
                if (winningSignal != null) {
                    SDominanceRelation r = SaltFactory.createSDominanceRelation();
                    r.setSource(secEdge);
                    r.setTarget(winningSignal);
                    this.getDocument().getDocumentGraph().addRelation(r);
                }
            }
        }

        private void markEdgesWithSignals() {
            for (SDominanceRelation dr : this.getDocument().getDocumentGraph().getDominanceRelations()) {
                if (! (dr.getTarget() instanceof SToken)
                        && (dr.getSource().getAnnotation("sec", "signaled_relation") == null)
                        && (dr.getSource().getAnnotation("prim", "signaled_relation") == null)
                        && (dr.getSource().getAnnotation("sec", "relname") == null)) {
                    dr.createAnnotation(null, "is_signaled",
                            this.primaryRelationsWithSignals.contains(dr));
                }
            }
        }

        @Override
        public DOCUMENT_STATUS mapSDocument() {
            // For some reason, salt doesn't have layer names ready by the time this manipulator is hit
            // To work around this, find an EDU and find its layer
            for (SNode n : this.getDocument().getDocumentGraph().getNodes()) {
                if (n.getAnnotation(null, "kind") != null) {
                    layer = (SLayer) n.getLayers().toArray()[0];
                }
            }
            indexUuids();
            constructSecEdges();
            constructSignals();
            connectSecondaryEdgesToSignals();
            markEdgesWithSignals();

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
