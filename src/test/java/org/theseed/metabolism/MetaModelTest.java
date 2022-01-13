/**
 *
 */
package org.theseed.metabolism;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;

/**
 * @author Bruce Parrello
 *
 */
public class MetaModelTest {

    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(MetaModelTest.class);


    @Test
    public void testCoordinates() {
        Coordinate xy1 = new Coordinate(1.0, 2.0);
        Coordinate xy2 = new Coordinate(2.0, 1.0);
        Coordinate xy3 = new Coordinate(-1.0, 2.0);
        Coordinate xy4 = new Coordinate(2.0, -1.0);
        Coordinate xy5 = new Coordinate(-1.0, -2.0); //
        Coordinate xy6 = new Coordinate(-2.0, -1.0);
        Coordinate xy7 = new Coordinate(1.0, -2.0);
        Coordinate xy8 = new Coordinate(-2.0, 1.0);
        var xySet = new TreeSet<Coordinate>();
        xySet.addAll(List.of(xy1, xy2, xy3, xy4, xy5, xy6, xy7, xy8));
        assertThat(xySet, contains(xy5, xy7, xy6, xy4, xy8, xy2, xy3, xy1));
        assertThat(xy1.getX(), equalTo(1.0));
        assertThat(xy1.getY(), equalTo(2.0));
        assertThat(xy1, not(equalTo(xy2)));
    }

    @Test
    public void testModel() throws IOException {
        File gFile = new File("data", "MG1655-wild.gto");
        File mFile = new File("data", "ecoli_cc.json");
        Genome genome = new Genome(gFile);
        MetaModel model = new MetaModel(mFile, genome);
        Collection<Reaction> reactions = model.getReactions("fig|511145.183.peg.2284");
        Optional<Reaction> targetReaction = reactions.stream().filter(x -> x.getBiggId().equals("CO2tex")).findFirst();
        assertThat("CO2tex not found.", targetReaction.isPresent());
        Reaction reaction0 = targetReaction.get();
        assertThat("Bad reversibility.", reaction0.isReversible());
        assertThat(reaction0.getName(), equalTo("CO2 transport via diffusion (extracellular to periplasm)"));
        assertThat(reaction0.getLabelLoc().getX(), closeTo(640.54504, 0.00001));
        assertThat(reaction0.getLabelLoc().getY(), closeTo(5774.13185, 0.00001));
        assertThat(reaction0.getReactionRule(), equalTo("b0241 or b0929 or b1377 or b2215"));
        assertThat(reaction0.getGenes(), containsInAnyOrder("b2215", "ompC", "b1377", "ompN", "b0929", "ompF",
                "b0241", "phoE"));
        List<Reaction.Stoich> parts = reaction0.getMetabolites();
        assertThat(parts.get(0).toString(), equalTo("co2_e"));
        assertThat(parts.get(0).getCoeff(), equalTo(1));
        assertThat("Not a reactant.", ! parts.get(0).isProduct());
        assertThat(parts.get(1).toString(), equalTo("co2_p"));
        assertThat(parts.get(1).getCoeff(), equalTo(1));
        assertThat("Not a product.", parts.get(1).isProduct());
        ModelNode.Metabolite metaNode0 = model.getPrimary("co2_e");
        assertThat(metaNode0.getId(), equalTo(2075513));
        assertThat(metaNode0.getLoc().getX(), closeTo(630.54504, 0.00001));
        assertThat(metaNode0.getLoc().getY(), closeTo(5884.13185, 0.00001));
        assertThat(metaNode0.getBiggId(), equalTo("co2_e"));
        assertThat("Node is not primary.", metaNode0.isPrimary());
        List<ModelNode.Metabolite> metaNodes = model.getMetabolites("co2_p");
        assertThat(metaNodes.size(), equalTo(2));
        // Verify the various reaction structures.
        var reactionMap = model.getReactionMap();
        Set<Reaction> orphans = model.getOrphanReactions();
        log.info("{} reactions were orphans.  {} features had associated reactions.", orphans.size(), reactionMap.size());
        log.info("{} metabolites present.  {} total map nodes.", model.getMetaboliteCount(), model.getNodeCount());
        Set<Reaction> all = model.getAllReactions();
        assertThat(all, hasItems(orphans.stream().toArray(Reaction[]::new)));
        for (Set<Reaction> reactionSet : reactionMap.values()) {
            for (Reaction reaction : reactionSet) {
                assertThat(reaction.toString(), reaction, not(in(orphans)));
                assertThat(reaction.toString(), reaction, in(all));
            }
        }
        // Verify the metabolite map.
        var metaMap = model.getMetaboliteMap();
        for (Map.Entry<String, List<ModelNode.Metabolite>> metaEntry : metaMap.entrySet()) {
            String metaBiggId = metaEntry.getKey();
            for (ModelNode.Metabolite metaNode : metaEntry.getValue()) {
                assertThat(metaNode.getBiggId(), equalTo(metaBiggId));
                ModelNode altCopy = model.getNode(metaNode.getId());
                assertThat(altCopy, sameInstance(metaNode));
            }
        }
        // Check the successor map.
        var successors = model.getSuccessors("frog");
        assertThat("Successors found for invalid metabolite.", successors.isEmpty());
        List<String> succNames = model.getSuccessors("thr__L_c").stream().map(x -> x.getBiggId()).collect(Collectors.toList());
        assertThat(succNames, containsInAnyOrder("THRD_L", "THRA", "THRD"));
        succNames = model.getSuccessors("2h3oppan_c").stream().map(x -> x.getBiggId()).collect(Collectors.toList());
        assertThat(succNames, containsInAnyOrder("HPYRI"));
        // Check the producer map.
        var producers = model.getProducers("frog");
        assertThat("Producers found for invalid metabolite.", producers.isEmpty());
        List<String> prodNames = model.getProducers("ser__L_c").stream().map(x  -> x.getBiggId()).collect(Collectors.toList());
        assertThat(prodNames, containsInAnyOrder("SERAT", "GHMT2r", "LSERDHr"));

    }

}
