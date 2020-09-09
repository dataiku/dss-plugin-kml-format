package com.dataiku.dss.utils;

import java.util.ArrayList;
import java.util.List;

import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.ProcessorOutput;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.RowFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.dataiku.dip.shaker.types.GeoPoint.Coords;

public class KMLParser {

    public KMLParser() {}

    public static Logger logger = Logger.getLogger("dku");

    public Element getFirstNodeByTagName(Element parent, String name){
        logger.info("Children of" + parent);
        for (int i=0; i < parent.getChildNodes().getLength(); i++){
            logger.info("Child "+i+":"+parent.getChildNodes().item(i));
        }
        NodeList nl = parent.getElementsByTagName(name);
        if (nl.getLength()==0) return null;
        else return (Element)nl.item(0);
    }

    public String getTextContent(Node e){return e.getTextContent();}

    private void putAttrValueIfExists(ColumnFactory cf, Row r, String columnName, Element e, String attrName) {
        String attrValue = e.getAttribute(attrName);
        if (!StringUtils.isBlank(attrValue)) {
            r.put(cf.column(columnName), attrValue);
        }
    }

    private void putContentIfExistsInChild(ColumnFactory cf, Row r, String columnName, Element e, String childNodeName) {
        Node childNode = getFirstNodeByTagName(e, childNodeName);
        if (childNode != null) {
            String txt = getTextContent(childNode);
            if (txt != null) r.put(cf.column(columnName), txt);
        }
    }

    private void parsePlacemark(Node node, ProcessorOutput out, ColumnFactory cf, RowFactory rf) throws Exception {
        Element e = (Element)node;
        //assert(node.getLocalName().equals("Placemark"));

        Row r = rf.row();

        putContentIfExistsInChild(cf, r, "name", e, "name");
        cf.column("id");
        putAttrValueIfExists(cf, r, "id", e, "id");

        cf.column("geom");

        {
            Element pointNode = getFirstNodeByTagName(e, "Point");
            if (pointNode != null) {
                Element coordsElt = getFirstNodeByTagName(pointNode, "coordinates");
                // Mandatory
                String coordsTxt = getTextContent(coordsElt);
                String[] chunks = coordsTxt.split(",");

                Coords coords = new Coords(Double.parseDouble(chunks[1]), Double.parseDouble(chunks[0]));
                r.put(cf.column("geom"), coords.toWKT());
            }
        }

        {
            Element linestringNode = getFirstNodeByTagName(e, "LineString");
            if (linestringNode != null) {
                Element coordsElt = getFirstNodeByTagName(linestringNode, "coordinates");
                // Mandatory
                String coordsTxt = getTextContent(coordsElt);
                logger.info("Parse linestring: " + coordsTxt);
                String[] points = StringUtils.splitByWholeSeparator(coordsTxt,  " ");

                List<String> pointsStr = new ArrayList<>();
                for (String point : points) {
                    if (StringUtils.isBlank(point)) continue;
                    logger.info("POINT: --" + point + "--");
                    String[] chunks = point.split(",");
                    pointsStr.add(chunks[1] + " " + chunks[0]);
                }
                r.put(cf.column("geom"), "LINESTRING(" + StringUtils.join(pointsStr, ",") + ")");
            }
        }

        {
            Element extendedDataElt = getFirstNodeByTagName(e, "ExtendedData");
            if (extendedDataElt != null) {
                NodeList nl = extendedDataElt.getElementsByTagName("Data");
                for (int i = 0; i < nl.getLength(); i++) {
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

        putContentIfExistsInChild(cf, r, "description", e, "description");
        putContentIfExistsInChild(cf, r, "snippet", e, "Snippet");
        putContentIfExistsInChild(cf, r, "address", e, "address");
        putContentIfExistsInChild(cf, r, "phoneNumber", e, "phoneNumber");

        out.emitRow(r);
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

}