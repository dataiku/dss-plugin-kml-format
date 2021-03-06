package com.dataiku.dss.utils;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.ProcessorOutput;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.RowFactory;
import com.dataiku.dip.shaker.types.GeoPoint;
import com.dataiku.dip.utils.DKULogger;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.dataiku.dip.shaker.types.GeoPoint.Coords;

public class KMLParser {

    public KMLParser() {}

    public Element getFirstNodeByTagName(Element parent, String name){
        NodeList nl = parent.getElementsByTagName(name);
        if (nl.getLength()==0) return null;
        else return (Element)nl.item(0);
    }

    private void putAttrValueIfExists(ColumnFactory cf, Row r, String columnName, Element e, String attrName) {
        String attrValue = e.getAttribute(attrName);
        if (!StringUtils.isBlank(attrValue)) {
            r.put(cf.column(columnName), attrValue);
        }
    }

    private void putContentIfExistsInChild(ColumnFactory cf, Row r, String columnName, Element e, String childNodeName) {
        Node childNode = getFirstNodeByTagName(e, childNodeName);
        if (childNode != null) {
            String txt = childNode.getTextContent();
            if (txt != null) {
                r.put(cf.column(columnName), txt);
            }
        }
    }

    private void parsePlacemark(Node node, ProcessorOutput out, ColumnFactory cf, RowFactory rf) throws Exception {
        if( node instanceof Element) {
            Element e = (Element)node;
            Row r = rf.row();
            putContentIfExistsInChild(cf, r, "name", e, "name");
            boolean foundPoint = extractAndSetPoint(cf, e, r);
            boolean foundLineString = extractAndSetLineString(cf, e, r);
            boolean foundPolygon = extractAndSetPolygon(cf, e, r);
            boolean foundParsableGeoObject = foundLineString || foundPoint || foundPolygon;
            if (foundParsableGeoObject){
                putAttrValueIfExists(cf, r, "id", e, "id");
                extractAndSetExtendedData(cf, e, r);
                putContentIfExistsInChild(cf, r, "description", e, "description");
                putContentIfExistsInChild(cf, r, "snippet", e, "Snippet");
                putContentIfExistsInChild(cf, r, "address", e, "address");
                putContentIfExistsInChild(cf, r, "phoneNumber", e, "phoneNumber");
                out.emitRow(r);
            } else {
                logger.infoV("Warning: A placemark has been skipped due to unsupported geospatial format.");
            }
        }
    }

    private void extractAndSetExtendedData(ColumnFactory cf, Element e, Row r) {
        Element extendedDataElt = getFirstNodeByTagName(e, "ExtendedData");
        if (extendedDataElt != null) {
            NodeList nl = extendedDataElt.getElementsByTagName("Data");
            if (nl != null){
                for (int i = 0; i < nl.getLength(); i++) {
                    if (nl.item(i) instanceof Element) {
                        Element dataElt = (Element)nl.item(i);
                        String dataName = dataElt.getAttribute("name");
                        if (!StringUtils.isBlank(dataName)) {
                            Element valueElt = getFirstNodeByTagName(dataElt, "value");
                            if (valueElt != null) {
                                String dataValue =  valueElt.getTextContent();
                                if (!StringUtils.isBlank(dataValue)) {
                                    r.put(cf.column(dataName), dataValue.trim());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean extractAndSetPoint(ColumnFactory cf, Element e, Row r) {
        boolean foundPoint = false;
        Element pointNode = getFirstNodeByTagName(e, "Point");
        if (pointNode != null) {
            Element coordsElt = getFirstNodeByTagName(pointNode, "coordinates");
            // Mandatory
            if (coordsElt != null){
                String coordsTxt = coordsElt.getTextContent();
                if (coordsTxt != null){
                    String[] chunks = coordsTxt.split(",");
                    if(chunks.length >= 2) {
                        Coords coords = new Coords(Double.parseDouble(chunks[1]), Double.parseDouble(chunks[0]));
                        r.put(cf.column("geom"), coords.toWKT());
                    }
                }
            }
            foundPoint = true;
        }
        return foundPoint;
    }

    private boolean extractAndSetLineString(ColumnFactory cf, Element e, Row r) {
        boolean foundLineString = false;
        Element linestringNode = getFirstNodeByTagName(e, "LineString");
        if (linestringNode != null) {
            Element coordsElt = getFirstNodeByTagName(linestringNode, "coordinates");
            // Mandatory
            if (coordsElt != null){
                String coordsTxt = coordsElt.getTextContent();
                if (coordsTxt != null){
                    String[] points = StringUtils.splitByWholeSeparator(coordsTxt,  " ");
                    if (points != null){
                        List<String> pointsStr = new ArrayList<>();
                        for (String point : points) {
                            if (StringUtils.isBlank(point)){
                                continue;
                            }
                            String[] chunks = point.split(",");
                            if(chunks.length >=2) {
                                formatGeoCoord(pointsStr, chunks);
                            }
                        }
                        if (pointsStr.size() >= 2){
                            r.put(cf.column("geom"), "LINESTRING(" + StringUtils.join(pointsStr, ",") + ")");
                        }
                    }
                }
            }
            foundLineString = true;
        }
        return foundLineString;
    }

    private void formatGeoCoord(List<String> pointsStr, String[] chunks) {
        double lat = Double.parseDouble(chunks[1]);
        double lng = Double.parseDouble(chunks[0]);
        DecimalFormat fmt = GeoPoint.getLatitudeLongitudeFormat();
        pointsStr.add(fmt.format(lng) + " " + fmt.format(lat));
    }

    private boolean extractAndSetPolygon(ColumnFactory cf, Element e, Row r) {
        boolean foundPolygon = false;
        Element linestringNode = getFirstNodeByTagName(e, "Polygon");
        if (linestringNode != null) {
            Element coordsElt = getFirstNodeByTagName(linestringNode, "coordinates");
            // Mandatory
            if (coordsElt != null){
                String coordsTxt = coordsElt.getTextContent();
                if (coordsTxt != null){
                    String[] points = StringUtils.splitByWholeSeparator(coordsTxt,  " ");
                    if (points != null){
                        List<String> pointsStr = new ArrayList<>();
                        for (String point : points) {
                            if (StringUtils.isBlank(point)){
                                continue;
                            }
                            String[] chunks = point.split(",");
                            if(chunks.length >=2) {
                                formatGeoCoord(pointsStr, chunks);
                            }
                        }
                        if (pointsStr.size() >= 2){
                            r.put(cf.column("geom"), "POLYGON((" + StringUtils.join(pointsStr, ",") + "))");
                        }
                    }
                }
            }
            foundPolygon = true;
        }
        return foundPolygon;
    }


    public void parseContainer(Node containerNode, ProcessorOutput out, ColumnFactory cf, RowFactory rf) throws Exception {
        for (int i = 0; i < containerNode.getChildNodes().getLength(); i++) {
            Node childNode = containerNode.getChildNodes().item(i);
            logger.info("Check child " + childNode);
            if (childNode instanceof Element) {
                Element childElt = (Element)childNode;
                logger.info("  " + childElt.getTagName());
                if (childElt.getTagName().equals("Placemark")) {
                    parsePlacemark(childNode, out, cf, rf);
                } else if (childElt.getTagName().equals("Folder")) {
                    parseContainer(childNode, out, cf, rf);
                }
            }
        }
    }

    private static DKULogger logger = DKULogger.getLogger("dku");
}