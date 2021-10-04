package pglogway;

import org.junit.Assert;
import org.junit.Test;

public class HourListTest {

	@Test
	public void empty() throws Exception {
		HourList hl=new HourList("");
		Assert.assertFalse(hl.in(24));
	}
	
	@Test
	public void range() throws Exception {
		HourList hl=new HourList("5-9");
		Assert.assertTrue(hl.in(5));
		Assert.assertTrue(hl.in(6));
		Assert.assertTrue(hl.in(9));
		Assert.assertFalse(hl.in(24));
	}
	
	@Test
	public void comma() throws Exception {
		HourList hl=new HourList("5,9");
		Assert.assertTrue(hl.in(5));
		Assert.assertTrue(hl.in(9));
		Assert.assertFalse(hl.in(6));
		Assert.assertFalse(hl.in(24));
	}
	
	@Test
	public void all() throws Exception {
		HourList hl=new HourList("5-9,11,21-24");
		Assert.assertTrue(hl.in(5));
		Assert.assertTrue(hl.in(9));
		Assert.assertTrue(hl.in(6));
		Assert.assertTrue(hl.in(11));
		Assert.assertTrue(hl.in(21));
		Assert.assertTrue(hl.in(24));
		Assert.assertFalse(hl.in(19));
	}

}
