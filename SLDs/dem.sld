<?xml version="1.0" ?>
<sld:StyledLayerDescriptor version="1.0.0" xmlns="http://www.opengis.net/sld" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:sld="http://www.opengis.net/sld">
    <sld:UserLayer>
        <sld:LayerFeatureConstraints>
            <sld:FeatureTypeConstraint/>
        </sld:LayerFeatureConstraints>
        <sld:UserStyle>
            <sld:Name>DEM</sld:Name>
            <sld:Title>DEM</sld:Title>
            <sld:FeatureTypeStyle>
                <sld:Name/>
                <sld:Rule>
                    <sld:RasterSymbolizer>
                        <sld:Geometry>
                            <ogc:PropertyName>grid</ogc:PropertyName>
                        </sld:Geometry>
                        <sld:Opacity>1</sld:Opacity>
                        <sld:ColorMap>
                            <sld:ColorMapEntry color="#97dfff" label="-250.000000" opacity="1.0" quantity="-250"/>
                            <sld:ColorMapEntry color="#fde8cf" label="75.000000" opacity="1.0" quantity="75"/>
                            <sld:ColorMapEntry color="#33a02c" label="400.000000" opacity="1.0" quantity="400"/>
                            <sld:ColorMapEntry color="#055500" label="725.000000" opacity="1.0" quantity="725"/>
                            <sld:ColorMapEntry color="#ff9f45" label="1050.000000" opacity="1.0" quantity="1050"/>
                            <sld:ColorMapEntry color="#664200" label="1375.000000" opacity="1.0" quantity="1375"/>
                            <sld:ColorMapEntry color="#593900" label="1700.000000" opacity="1.0" quantity="1700"/>
                            <sld:ColorMapEntry color="#4d3000" label="2025.000000" opacity="1.0" quantity="2025"/>
                            <sld:ColorMapEntry color="#887555" label="2350.000000" opacity="1.0" quantity="2350"/>
                            <sld:ColorMapEntry color="#c3baaa" label="2675.000000" opacity="1.0" quantity="2675"/>
                            <sld:ColorMapEntry color="#ffffff" label="3000.000000" opacity="1.0" quantity="3000"/>
                        </sld:ColorMap>
                    </sld:RasterSymbolizer>
                </sld:Rule>
            </sld:FeatureTypeStyle>
        </sld:UserStyle>
    </sld:UserLayer>
</sld:StyledLayerDescriptor>
