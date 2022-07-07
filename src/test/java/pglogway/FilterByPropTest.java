package pglogway;

import static org.junit.Assert.*;

import org.junit.Test;

public class FilterByPropTest {

	@Test
	public void test() throws Exception {
		FilterByProp p=new FilterByProp("hede", "-BEGIN,-SET");
		assertTrue(p.filter("BEGIN"));
		assertTrue(p.filter("SET"));
		assertFalse(p.filter("BEGI"));
	}

}
