package uk.org.nbn.util;

import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import scala.Option;

public class GridUtilTests {

  @ParameterizedTest
  @CsvSource({
    "NM39, NM, 130000, 790000, 10000, 130000, 790000, 140000, 800000, EPSG:27700",
    "NM4099, NM,140000,799000, 1000, 140000, 799000, 141000, 800000, EPSG:27700",
    "NG316005, NG,131600,800500, 100,131600,800500,131700,800600, EPSG:27700",
    "NM39A, NM,130000,790000,2000,130000,790000,132000,792000, EPSG:27700",
    "NM39E, NM,130000,798000,2000,130000,798000,132000,800000, EPSG:27700",
    "NM39G, NM,132000,792000,2000,132000,792000,134000,794000, EPSG:27700",
    "NM39S, NM,136000,794000,2000,136000,794000,138000,796000, EPSG:27700",
    "NM39N, NM,134000,796000,2000,134000,796000,136000,798000, EPSG:27700",
    "NM39P, NM,134000,798000,2000,134000,798000,136000,800000, EPSG:27700",
    "NM39Z, NM,138000,798000,2000,138000,798000,140000,800000, EPSG:27700"
  })
  public void convertOSGridReferenceToEastingAndNorthing(
      String gridRefernce,
      String gridLetters,
      int easting,
      int northing,
      Integer gridSize,
      int minEasting,
      int minNorthing,
      int maxEasting,
      int maxNorthing,
      String datum) {

    GridRef result = GridUtil.gridReferenceToEastingNorthing(gridRefernce).getOrElse(null);
    GridRef expected =
        new GridRef(
            gridLetters,
            easting,
            northing,
            Option.apply(gridSize),
            minEasting,
            minNorthing,
            maxEasting,
            maxNorthing,
            datum);

    Assert.assertEquals(expected, result);
  }

  @ParameterizedTest
  @CsvSource({
    "J4967, 54.529443, -5.699145",
    "IJ4967, 54.529443, -5.699145",
    "H99, 54.793876, -6.523798"
  })
  public void convertIrishGridReferenceToEastingAndNorthing(
      String gridRefernce, String minLatitude, String minLongitude) {

    GISPoint result = GridUtil.processGridReference(gridRefernce).getOrElse(null);

    Assert.assertNotNull(result);
    Assert.assertEquals(minLatitude, result.minLatitude());
    Assert.assertEquals(minLongitude, result.minLongitude());
  }

  @ParameterizedTest
  @CsvSource({"H99, 290000, 390000,  54.793876, -6.523798"})
  public void convertIrishGridReferenceToEastingAndNorthing2(
      String gridRefernce, String easting, String northing, String latitude, String longitude) {

    GISPoint result = GridUtil.processGridReference(gridRefernce).getOrElse(null);

    Assert.assertNotNull(result);
    Assert.assertEquals(easting, result.easting());
    Assert.assertEquals(northing, result.northing());
    Assert.assertEquals(latitude, result.latitude());
    Assert.assertEquals(longitude, result.longitude());
  }

  @ParameterizedTest
  @CsvSource({"NM39, 56.970009, -6.361995, EPSG:4326, 7071.1"})
  public void convertOSGridTOLatLonWGS84(
      String gridReference, String latitude, String longitude, String datum, String uncertainty) {

    GISPoint result = GridUtil.processGridReference(gridReference).getOrElse(null);

    Assert.assertNotNull(result);
    Assert.assertEquals(latitude, result.latitude());
    Assert.assertEquals(longitude, result.longitude());
    Assert.assertEquals(datum, result.datum());
    Assert.assertEquals(uncertainty, result.coordinateUncertaintyInMeters());
  }

  @ParameterizedTest
  @CsvSource({
    "NH123123, NH, NH11, -, NH1212, NH123123",
    "NH12341234, NH, NH11, -, NH1212, NH123123",
    "NH1234512345, NH, NH11, NH11G, NH1212, NH123123",
    "J12341234, J, J11, -, J1212, J123123",
    "J43214321, J, J44, J44G, J4343, J432432"
  })
  public void checkGridRefenceAtDifferentResolutions(
      String gridReference,
      String grid_ref_100000,
      String grid_ref_10000,
      String grid_ref_2000,
      String grid_ref_1000,
      String grid_ref_100) {

    Map<String, String> result = GridUtil.getGridRefAsResolutions(gridReference);

    Assert.assertNotNull(result);
    Assert.assertEquals(grid_ref_100000, result.get("grid_ref_100000"));
    Assert.assertEquals(grid_ref_10000, result.get("grid_ref_10000"));
    Assert.assertEquals(grid_ref_1000, result.get("grid_ref_1000"));
    Assert.assertEquals(grid_ref_100, result.get("grid_ref_100"));

    if (!grid_ref_2000.equals("-")) {
      Assert.assertEquals(grid_ref_2000, result.get("grid_ref_2000"));
    }
  }

  @ParameterizedTest
  @CsvSource({"J43G, J, J43, J43G", "C12Q, C, C12, C12Q", "NH12Q, NH, NH12, NH12Q"})
  public void twoKmGridReferencesAtDifferentResolutions(
      String gridReference, String grid_ref_100000, String grid_ref_10000, String grid_ref_2000) {

    GridRef eastingNorthing =
        GridUtil.gridReferenceToEastingNorthing(gridReference).getOrElse(null);
    Assert.assertNotNull(eastingNorthing);

    String newGridRef =
        eastingNorthing.gridLetters()
            + String.valueOf(eastingNorthing.easting()).substring(1)
            + String.valueOf(eastingNorthing.northing()).substring(1);

    Map<String, String> result = GridUtil.getGridRefAsResolutions(newGridRef);

    Assert.assertNotNull(result);
    Assert.assertEquals(grid_ref_100000, result.get("grid_ref_100000"));
    Assert.assertEquals(grid_ref_10000, result.get("grid_ref_10000"));
    Assert.assertEquals(grid_ref_2000, result.get("grid_ref_2000"));
  }

  @ParameterizedTest
  @CsvSource({
    "SJ, 53.36916, -2.69094, 100000, WGS84, OSGB",
    "SJ58, 53.36916, -2.69094, 5000, WGS84, OSGB",
    "SJ5486, 53.36916, -2.69094, 500, WGS84, OSGB",
    "SM886385, 52.00464, -5.081357, 70, WGS84, OSGB",
    "SM880395, 52.01419, -5.09049, 70, WGS84, OSGB",
    "SJ, 53.36916, -2.69094, 100000, EPSG:27700, OSGB",
    "SJ58, 53.36916, -2.69094, 7000, EPSG:27700, OSGB",
    "SJ5486, 53.36916, -2.69094, 700, EPSG:27700, OSGB",
    "TG51401317, 52.65757, 1.71791, 7, EPSG:27700, OSGB",
    "TG5140913177, 52.65757, 1.71791, 0.7, EPSG:27700, OSGB",
    "D05530565, 54.88744, -6.3562, 7, WGS84, Irish",
    "H95698720, 54.72375, -6.51556, 7, WGS84, Irish",
    "H82957082, 54.57889, -6.71781, 7, WGS84, Irish"
  })
  public void latLonToOSGridAtDifferentResolutions(
      String expectedGridReference,
      Double latitude,
      Double longitude,
      Double gridSize,
      String datum,
      String gridType) {

    String result =
        GridUtil.latLonToOsGrid(latitude, longitude, gridSize, datum, gridType, -1).getOrElse(null);

    Assert.assertEquals(expectedGridReference, result);
  }

  @ParameterizedTest
  @CsvSource({
    "true, -2.809, 52.2824, SO46",
    "false, -2.7888, 52.2971,SO46",
    "true, -2.78593, 52.294,SO4666",
    "false, -2.78593, 52.29494,SO4666",
    "true, -2.7877410, 52.2964728, SO46376677",
    "false, -2.7877547, 52.2964728,SO46376677",
    "true, -6.79750, 54.99451,C71T",
    "false, -6.80109, 54.99451,C71T",
    "false, -6.80109, 54.99451,XXXX",
    "false, -5.80109, 51.99451,C71T"
  })
  public void gridCentroid(
      boolean expectedResult, double longitude, double latitude, String gridReference) {
    boolean result = GridUtil.isCentroid(longitude, latitude, gridReference);
    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public void _1mGridRoundingErrorTest() {
    GridRef grid = GridUtil.gridReferenceToEastingNorthing("ND2135464379").get();
    String result =
        GridUtil.getOSGridFromNorthingEasting(grid.northing(), grid.easting(), 10, "OSGB").get();

    Assert.assertEquals("ND2135464379", result);
  }

  @Test
  public void _1mGridRoundingErrorTest_2() {
    String result = GridUtil.latLonToOsGrid(58.56032, -3.35343, 0.7, "WGS84", "OSGB", 1).get();

    Assert.assertEquals("ND2135464379", result);
  }

  @ParameterizedTest
  @CsvSource({
    "1, 0.7",
    "10, 7",
    "100, 70.7",
    "1000, 707.1",
    "2000, 1414",
    "10000, 7071",
    "10, 1.45",
    "100, 10.45",
    "1000, 75.45",
    "1000, 101.45",
    "2000, 899.45",
    "10000, 2201.45",
    "10, 1"
  })
  public void gridSizeForCoordinateUncertainty(Integer expectedGridSize, Double uncertainty) {
    Integer result = GridUtil.calculateGridSize(uncertainty);
    Assert.assertEquals(expectedGridSize, result);
  }

  @ParameterizedTest
  @CsvSource({
    "10, 1",
    "8, 10",
    "6, 100",
    "4, 1000",
    "3, 2000",
    "2, 10000",
    "0, 100000",
    "0, 200000",
    "0, 50000"
  })
  public void gridReferenceDigitsForGridSize(Integer expectedDigits, Integer gridSize) {
    Integer digits = GridUtil.calculateNumOfGridRefDigits(gridSize);
    Assert.assertEquals(expectedDigits, digits);
  }

  @ParameterizedTest
  @CsvSource({
    "10, 0.7",
    "8, 7",
    "6, 70.7",
    "4, 707.1",
    "3, 1414",
    "2, 7071",
    "8, 1.45",
    "6, 10.45",
    "4, 75.45",
    "4, 101.45",
    "3, 899.45",
    "2, 2201.45",
    "8, 1"
  })
  public void gridReferenceDigitsForCoordinateUncertainty(
      Integer expectedDigits, Double uncertainty) {
    Integer digits = GridUtil.calculateNumOfGridRefDigits(GridUtil.calculateGridSize(uncertainty));
    Assert.assertEquals(expectedDigits, digits);
  }
}
