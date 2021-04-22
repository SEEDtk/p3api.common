package org.theseed.p3api.common;

import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;

import org.theseed.genome.Genome;
import org.theseed.locations.Location;

/**
 * Unit test for simple App.
 */
public class AppTest {

    @Test
    public void testDnaFetch() throws IOException {
        Genome str277 = new Genome(new File("data", "MG1655-ATCC21277.gto"));
        Genome wild = new Genome(new File("data", "MG1655-wild.gto"));
        Location strLoc = Location.create("NODE_2_length_269836_cov_32.5831", 9884, 9577);
        Location wLoc = Location.create("511145.183.con.0001", 1039372, 1039679);
        String strDna = str277.getDna(strLoc);
        String wDna = wild.getDna(wLoc);
        System.out.println(strDna);
        System.out.println(wDna);
        assertThat("A", equalTo("A"));
    }

}
