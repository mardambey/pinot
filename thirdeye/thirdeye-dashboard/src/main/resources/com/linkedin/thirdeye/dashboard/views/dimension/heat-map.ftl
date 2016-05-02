<script src="/assets/js/thirdeye.dimension.heatmap.js"></script>
<script type="text/javascript" src="https://www.google.com/jsapi"></script>


<div id="dimension-heat-map-area">

      <#list dimensionView.view.metricNames as metric>
      <div class="metric-section-wrapper" rel="${metric}">
         <div class="dimension-heat-map-treemap-section">
              <div class="title-box">
                  <table>
                        <tr>
                            <th><p>${metric}</p></th>
                            <th> Baseline Date: </th>
                            <th> Current Date: </th>
                            <th> Baseline Total:</th>
                            <th> Current Total:</th>
                            <th> Delta Value:</th>
                            <th> Delta (%):</th>
                            </tr>
                        <tr>
                            <td></td>
                            <td class="title-stat baseline-date-time"></td>
                            <td class="title-stat current-date-time"></td>
                            <td class="title-stat baseline-total">${dimensionView.view.metricGlobalStats[metric]['baseline_total']}</td>
                            <td class="title-stat current-total">${dimensionView.view.metricGlobalStats[metric]['current_total']}</td>
                            <td class="title-stat delta-value">${dimensionView.view.metricGlobalStats[metric]['delta_absolute_change']}</td>
                            <td class="title-stat delta-ratio">${dimensionView.view.metricGlobalStats[metric]['delta_percent_change']}</td>
                        </tr>
                  </table>
              </div>
              <div class="uk-button-group dimension-treemap-toggle-buttons" data-uk-button-radio>
                    <button class="uk-button dimension-treemap-mode" id="treemap_contribution-total-change-percent" mode="0">
                        <i class="uk-icon-eye-slash"></i> Percentage Change
                    </button>
                    <button class="uk-button dimension-treemap-mode" id="treemap_contribution-total-percent" mode="1">
                        <i class="uk-icon-eye-slash"></i> Contribution Change (%)
                    </button>
                    <button class="uk-button dimension-treemap-mode" id="treemap_contribution-change-percent" mode="2">
                        <i class="uk-icon-eye-slash"></i> Contribution to overall Change (%)
                    </button>
              </div>

              <div id="metric_${metric?index}_treemap_0" class="treemap-container" mode="0">
                    <table class="treemap-display-tbl">

                    <#list dimensionView.view.dimensionNames as dimension>
                              <tr>
                                  <td class="treemap-display-tbl-dim"><div class="treemap-rotate"><div>${dimension}</div></div></td><td id="metric_${metric?index}_dim_${dimension?index}_treemap_0" class="dimension-treemap" rel="${dimension}" style="width: 100%; height:100px;display:inline-block" ></td>
                              </tr>
                    </#list>
                    </table>

                </div>
                <div id="metric_${metric?index}_treemap_1" class="treemap-container" mode="1">
                    <table class="treemap-display-tbl">

                    <#list dimensionView.view.dimensionNames as dimension>
                              <tr>
                                  <td class="treemap-display-tbl-dim"><div class="treemap-rotate"><div>${dimension}</div></div></td><td id="metric_${metric?index}_dim_${dimension?index}_treemap_1" class="dimension-treemap" rel="${dimension}" style="width: 100%; height:100px;display:inline-block" ></td>
                              </tr>
                    </#list>
                    </table>

                </div>
                <div id="metric_${metric?index}_treemap_2" class="treemap-container" mode="2">
                    <table class="treemap-display-tbl">

                    <#list dimensionView.view.dimensionNames as dimension>
                              <tr>
                                  <td class="treemap-display-tbl-dim"><div class="treemap-rotate"><div>${dimension}</div></div></td><td id="metric_${metric?index}_dim_${dimension?index}_treemap_2" class="dimension-treemap" rel="${dimension}" style="width: 100%; height:100px;display:inline-block" ></td>
                              </tr>
                    </#list>
                    </table>
              </div>
          </div>

          <div id="dimension-heat-map-table-section" class="dimension-heat-map-table-section hidden" rel="table-section">
                   <div id='div-tabularView-metric_${metric?index}' class="title-box">
                       <table style="width:100%; table-layout:fixed;">
                           <tr>
                               <th><p>${metric}</p></th>
                               <th> Baseline Date: </th>
                               <th> Current Date: </th>
                               <th> Baseline Total:</th>
                               <th> Current Total:</th>
                               <th> Delta Value:</th>
                               <th> Delta (%):</th>
                           </tr>
                           <tr>
                               <td></td>
                               <td class="title-stat baseline-date-time"></td>
                               <td class="title-stat current-date-time"></td>
                               <td class="title-stat baseline-total">${dimensionView.view.metricGlobalStats[metric]['baseline_total']}</td>
                               <td class="title-stat current-total">${dimensionView.view.metricGlobalStats[metric]['current_total']}</td>
                               <td class="title-stat delta-value">${dimensionView.view.metricGlobalStats[metric]['delta_absolute_change']}</td>
                               <td class="title-stat delta-ratio">${dimensionView.view.metricGlobalStats[metric]['delta_percent_change']}</td>
                           </tr>
                       </table>
                   </div>
                    <div class="uk-button-group dimension-tabular-column-toggle-buttons" data-uk-button-checkbox>
                        <button class="uk-button toggle-vis" data-column="7" rel="contribution-total-change-percent">
                            <i class="uk-icon-eye-slash"></i> Contribution to Total Change (%)
                        </button>
                        <button class="uk-button toggle-vis" data-column="8" rel="contribution-total-percent">
                            <i class="uk-icon-eye-slash"></i> Contribution to Total (%)
                        </button>
                        <button class="uk-button toggle-vis" data-column="9" rel="contribution-change-percent">
                            <i class="uk-icon-eye-slash"></i> Contribution Change (%)
                        </button>
                    </div>

                   <table id='tabularView-metric_${metric?index}' class="display compact" cell-spacing="0" width="100%">
                        <thead>
                        <tr id="filter-header" class="filter-header">
                            <th class="filter-header-m" rel="dimension">Dimension</th>
                            <th id="rank" class="filter-header-s" rel="rank">Rank</th>
                            <th           class="filter-header-m" rel="dimension-value">Dimension Value</th>
                            <th           class="filter-header-s" rel="baseline">Baseline</th>
                            <th           class="filter-header-s" rel="current">Current</th>
                            <th           class="filter-header-s" rel="delta-val">Delta (Value)</th>
                            <th           class="filter-header-s" rel="delta-ratio">Delta (%)</th>
                            <th           class="filter-header-s" rel="contribution_to_total_change_val">Contribution to Total Change</th>
                            <th           class="filter-header-s" rel="contribution_to_total_change_ratio">Contribution to Total (%)</th>
                            <th           class="filter-header-s" rel="contribution_change_ratio">Contribution Change (%)</th>
                        </tr>
                        <tr>
                            <th>Dimension</th>
                            <th>Rank</th>
                            <th>Dimension Value</th>
                            <th>Baseline</th>
                            <th>Current</th>
                            <th>Delta (Value)</th>
                            <th>Delta (%)</th>
                            <th>Contribution to Total Change (%)</th>
                            <th>Contribution to Total (%)</th>
                            <th>Contribution Change (%)</th>
                        </tr>

                        </thead>

                        <tbody>
                            <#list dimensionView.view.heatMaps as heatMap>
                                <#list heatMap.cells as cell>
                                <#if (heatMap.metric == metric)>
                                <tr role="row" class="${cell?item_cycle('even','odd')}">
                                    <td>${heatMap.dimension}</td>
                                    <td>${cell?index}</td>
                                    <#if (cell.value?html == "")>
                                        <td dimension-val class="datatable-empty-cell"></td>
                                    <#else>
                                        <td dimension-val>${cell.value?html}</td>
                                    </#if>
                                    <td>${cell.statsMap['baseline_value']?string["0.##"]}</td>
                                    <td>${cell.statsMap['current_value']?string["0.##"]}</td>
                                    <td>${(cell.statsMap['current_value'] - cell.statsMap['baseline_value'])?string["0.##"]} </td>
                                    <#if (cell.statsMap['baseline_value'] > 0)>
                                      <td>${(cell.statsMap['current_value'] - cell.statsMap['baseline_value'])/(cell.statsMap['baseline_value']) * 100}</td>
                                    <#else>
                                      <td>0</td>
                                    </#if>
                                    <td> ${cell.statsMap['volume_difference'] * 100}</td>
                                    <td> ${cell.statsMap['current_ratio'] * 100}</td>
                                    <td> ${cell.statsMap['contribution_difference'] * 100}</td>
                                </tr>
                                </#if>
                                </#list>
                            </#list>
                      </tbody>
                      <tfoot>
                      <tr>
                            <th>Dimension</th>
                            <th>Rank</th>
                            <th>Dimension Value</th>
                            <th>Baseline</th>
                            <th>Current</th>
                            <th>Delta (Value)</th>
                            <th>Delta (%)</th>
                            <th>Contribution to Total Change (%)</th>
                            <th>Contribution to Total (%)</th>
                            <th>Contribution Change (%)</th>
                        </tr>
                      </tfoot>
                </table>
          </div>
      </div>
      </#list>
</div>

<script>
    //Creating the heatmap and the tootlip data
    var Treemap = {

        drawChart : function() {
            var Tooltip = {

            <#global idx =0>
                tooltipData : google.visualization.arrayToDataTable([
                    ['id',  'metric','dimension','cellvalue','baseline_value', 'current_value','baseline_ratio', 'current_ratio','delta_percent_change', 'contribution_difference', 'volume_difference' ],
                <#list dimensionView.view.metricNames as metric>
                    <#list dimensionView.view.dimensionNames as dimension>
                        <#list dimensionView.view.heatMaps as heatMap>
                            <#if (heatMap.metric == metric && heatMap.dimension == dimension)>
                                <#list heatMap.cells as cell>
                                    <#if (cell.statsMap['current_ratio'] *100 > 1 )>
                                        [ '${idx}','${metric}', '${dimension}', '${cell.value?html}', ${cell.statsMap['baseline_value']?c}, ${cell.statsMap['current_value']?c}, ${cell.statsMap['baseline_ratio']?c},${cell.statsMap['current_ratio']?c} ,${cell.statsMap['delta_percent_change']?c}, ${cell.statsMap['contribution_difference']?c}, ${cell.statsMap['volume_difference']?c}],
                                        <#global  idx = idx + 1>
                                    </#if>
                                </#list>

                            </#if>
                        </#list>
                    </#list>
                </#list>
                ]),

                showTreeMapTooltip : function(row, size, value){

                    var currentTable = $(this.ma).attr('id');
                    var tableMetricDim = currentTable.substr(0, currentTable.length -10);

                    var dataMode = currentTable.substr(currentTable.length -1, 1);
                    var dataTable = tableMetricDim + '_data_' + dataMode;
                    var indexStr =  Treemap[ dataTable ].getValue(row, 0);
                    if(isNaN(indexStr)){
                        return "";
                    }
                    var index = Number(indexStr);

                    //Tooltip Data columns
                    //['id',  'metric','dimension','cellvalue','baseline_value', 'current_value','baseline_ratio', 'current_ratio','delta_percent_change', 'contribution_difference', 'volume_difference' ],
                    var cellValue = Tooltip.tooltipData.getValue(Number(index), 3) == "" ? "unknown" : Tooltip.tooltipData.getValue(Number(index), 3)
                    var baseline = (Tooltip.tooltipData.getValue(Number(index), 4)).toFixed(2).replace(/\.?0+$/,'')
                    var current = (Tooltip.tooltipData.getValue(Number(index), 5)).toFixed(2).replace(/\.?0+$/,'')
                    var deltaValue = (current - baseline).toFixed(2).replace(/\.?0+$/,'')
                    var currentRatio = (Tooltip.tooltipData.getValue(Number(index), 7) * 100).toFixed(2).replace(/\.?0+$/,'');
                    var contributionDifference = (Tooltip.tooltipData.getValue(Number(index), 9) * 100).toFixed(2).replace(/\.?0+$/,'');
                    return '<div class="treemap-tooltip">' +
                            '<table>' +
                            '<tr><td>value : </td><td><a class="tooltip-link" href="#" rel="'+ Tooltip.tooltipData.getValue(Number(index), 2) +'">' +  cellValue + '</a></td></tr>' +
                            '<tr><td>baseline value : </td><td>' +  baseline + '</td></tr>' +
                            '<tr><td>current value : </td><td>'+ current + '</td></tr>' +
                            '<tr><td>delta value : </td><td>' + deltaValue + '</td></tr>' +
                            '<tr><td>delta (%) : </td><td>' + (Tooltip.tooltipData.getValue(Number(index), 8) *100).toFixed(2) + '</td></tr>' +
                            '<tr><td>change in contribution (%) : </td><td>' + currentRatio +'(' + contributionDifference +')' + '</td></tr>' +
                            '</table>' +
                            '</div>'
                }

            }
            var CreateNewQuery = {
                //Google Charts is catching the click event on the treemap cells and
                //provides the select event without the clickec cell display value and dimension value so we catch them on mousedown as
                //CreateNewQuery.dimension, CreateNewQuery.value
                mouseDownHandler : function(){
                  // TODO: click on the cell of the treemap should fix the value and dimension in the query and rerender the page
                },
                selectHandler : function(){
                 // TODO: click on the cell of the treemap should fix the value and dimension in the query and rerender the page
                }
            }

            var options = {
                maxDepth: 2,
                minColorValue: -25,
                maxColorValue: 25,
                minColor: '#f00',
                midColor: '#ddd',
                maxColor: '#00f',
                headerHeight: 0,
                fontColor: '#000',
                showScale: false,
                highlightOnMouseOver: true,
                generateTooltip : Tooltip.showTreeMapTooltip
            }


        /**
         * Each cell in a data table has a Value(used as the id) and FormattedValue(what is shown).
         **/
        <#global idx_0 =0>
        <#global idx_1 =0>
        <#global idx_2 =0>
        <#list dimensionView.view.metricNames as metric>
            <#list dimensionView.view.dimensionNames as dimension>
            Treemap.metric_${metric?index}_dim_${dimension?index}_data_0 = google.visualization.arrayToDataTable([
                [{v:'uniqueID', f:'displayValue'},  'Parent', 'current ratio (size)', 'delta (color)'],
                [{v:'${metric}_${dimension}', f:'${dimension}'}, null, 0,  ${dimension?index}],
                <#list dimensionView.view.heatMaps as heatMap>
                    <#if (heatMap.metric == metric && heatMap.dimension == dimension)>
                        <#list heatMap.cells as cell>
                            <#if (cell.statsMap['current_ratio'] *100 > 1 )>
                                <#if (cell.value?has_content)>
                                    [{ v:'${.globals.idx_0}', f:'${cell.value} (${(cell.statsMap['delta_percent_change'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['delta_percent_change'] * 100)?c}],
                                <#else>
                                    [{v:'${.globals.idx_0}', f:'unknown (${(cell.statsMap['delta_percent_change'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['delta_percent_change'] * 100)?c}],
                                </#if>
                                <#global  idx_0 = idx_0 + 1>
                            </#if>
                        </#list> //end of heatmap Cell
                    </#if>
                </#list> //end of heatmap
            ]);

                Treemap.metric_${metric?index}_dim_${dimension?index}_data_1 = google.visualization.arrayToDataTable([
                    [{v:'uniqueID', f:'displayValue'},  'Parent', 'current ratio (size)', 'delta (color)'],
                    [{v:'${metric}_${dimension}', f:'${dimension}'}, null, 0,  ${dimension?index}],
                    <#list dimensionView.view.heatMaps as heatMap>
                        <#if (heatMap.metric == metric && heatMap.dimension == dimension)>
                            <#list heatMap.cells as cell>
                                <#if (cell.statsMap['current_ratio'] *100 > 1 )>
                                    <#if (cell.value?has_content)>
                                        [{v:'${.globals.idx_1}', f:'${cell.value} (${(cell.statsMap['contribution_difference'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['contribution_difference'] * 100)?c}],
                                    <#else>
                                        [{v:'${.globals.idx_1}', f:'unknown (${(cell.statsMap['contribution_difference'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['contribution_difference'] * 100)?c}],
                                    </#if>
                                    <#global  idx_1 = idx_1 + 1>
                                </#if>
                            </#list> //end of heatmap Cell
                        </#if>
                    </#list> //end of heatmap
                ]);
                Treemap.metric_${metric?index}_dim_${dimension?index}_data_2 = google.visualization.arrayToDataTable([
                    [{v:'uniqueID', f:'displayValue'},  'Parent', 'current ratio (size)', 'delta (color)'],
                    [{v:'${metric}_${dimension}', f:'${dimension}'}, null, 0,  ${dimension?index}],
                    <#list dimensionView.view.heatMaps as heatMap>
                        <#if (heatMap.metric == metric && heatMap.dimension == dimension)>
                            <#list heatMap.cells as cell>
                                <#if (cell.statsMap['current_ratio'] *100 > 1 )>
                                    <#if (cell.value?has_content)>
                                        [{v:'${.globals.idx_2}', f:'${cell.value} (${(cell.statsMap['volume_difference'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['volume_difference'] * 100)?c}],
                                    <#else>
                                        [{v:'${.globals.idx_2}', f:'unknown (${(cell.statsMap['volume_difference'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['volume_difference'] * 100)?c}],
                                    </#if>
                                    <#global idx_2 = idx_2 + 1>
                                </#if>
                            </#list> //end of heatmap Cell
                        </#if>
                    </#list> //end of heatmap
                ]);
                //treemap where 2 level depth is displayed
                var metric_${metric?index}_dim_${dimension?index}_treemap_0 = new google.visualization.TreeMap(document.getElementById('metric_${metric?index}_dim_${dimension?index}_treemap_0'));
                var metric_${metric?index}_dim_${dimension?index}_treemap_1 = new google.visualization.TreeMap(document.getElementById('metric_${metric?index}_dim_${dimension?index}_treemap_1'));
                var metric_${metric?index}_dim_${dimension?index}_treemap_2 = new google.visualization.TreeMap(document.getElementById('metric_${metric?index}_dim_${dimension?index}_treemap_2'));


                metric_${metric?index}_dim_${dimension?index}_treemap_0.draw(Treemap.metric_${metric?index}_dim_${dimension?index}_data_0, options);
                metric_${metric?index}_dim_${dimension?index}_treemap_1.draw(Treemap.metric_${metric?index}_dim_${dimension?index}_data_1, options);
                metric_${metric?index}_dim_${dimension?index}_treemap_2.draw(Treemap.metric_${metric?index}_dim_${dimension?index}_data_2, options);

                google.visualization.events.addListener( metric_${metric?index}_dim_${dimension?index}_treemap_0 , 'select', CreateNewQuery.selectHandler);
                google.visualization.events.addListener( metric_${metric?index}_dim_${dimension?index}_treemap_1 , 'select', CreateNewQuery.selectHandler);
                google.visualization.events.addListener( metric_${metric?index}_dim_${dimension?index}_treemap_2 , 'select', CreateNewQuery.selectHandler);

            </#list> //end of dimension
        </#list>  //end of metric



            //Preselect treeemap mode on pageload (mode 0 = Percentage Change)
            $(".dimension-treemap-mode[mode = '0']").click()

            $(".tooltip-link").click(function(e){

                //prevent the default jump to top behavior on <a href="#"></a>
                e.preventDefault()

                //get value and dimension from the current tooltip
                var value = ($(this).html().trim() == "unknown") ? "" : $(this).html().trim();
                var dimension = $(this).attr("rel");

               /* earlier version when the URI was handled as an object
               //fix the value and dimension in the query and redraw the page
                var dimensionValues = parseDimensionValues(window.location.search)
                //setting the dimension's query to 1 dimension value only
                dimensionValues[dimension] = value
                console.log("encodeDimensionValues(dimensionValues)", encodeDimensionValues(dimensionValues))
                window.location.search = encodeDimensionValues(dimensionValues)*/

                 //fix the value and dimension in the query and redraw the page
                var dimensionValues = parseDimensionValuesAry(window.location.search)
                //setting the dimension's query to 1 dimension value only
                dimensionValues.push( dimension + "=" + value )
                console.log("encodeDimensionValues(dimensionValues)", encodeDimensionValues(dimensionValues))
                window.location.search = encodeDimensionValuesAry(dimensionValues)
            })

            $(".treemap-container svg").on("mousedown", "g", CreateNewQuery.mouseDownHandler)

                //Preselect treeemap mode on pageload (mode 0 = Percentage Change)
               $(".dimension-treemap-mode[mode = '0']").click()


            //After all the content loaded set the fontcolor of the cells based on the brightness of the background color
            function fontColorOverride(cell) {

                var cellObj = $(cell);
                var hex = cellObj.attr("fill");

                var colorIsLight = function (r, g, b) {
                    // Counting the perceptive luminance
                    // human eye favors green color...
                    var a = 1 - (0.299 * r + 0.587 * g + 0.114 * b) / 255;
                    return (a < 0.5);
                }


                function hexToRgb(hex) {
                    // Expand shorthand form (e.g. "03F") to full form (e.g. "0033FF")
                    var shorthandRegex = /^#?([a-f\d])([a-f\d])([a-f\d])$/i;
                    hex = hex.replace(shorthandRegex, function (m, r, g, b) {
                        return r + r + g + g + b + b;
                    });

                    var result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
                    return result ? {
                        r: parseInt(result[1], 16),
                        g: parseInt(result[2], 16),
                        b: parseInt(result[3], 16)
                    } : null;
                }

                if (hexToRgb(hex)) {
                    var textColor = colorIsLight(hexToRgb(hex).r, hexToRgb(hex).g, hexToRgb(hex).b) ? '#000000' : '#ffffff';
                    cellObj.next('text').attr('fill', textColor);
                }

            };
            var area = $("#dimension-heat-map-area")
            $("rect", area).each( function(index,cell){
                fontColorOverride(cell);
            });

            $("g", area).on("mouseout", function(event){
                if($(event.currentTarget).prop("tagName") === "g"){
                    fontColorOverride($("rect", event.currentTarget));
                }
            });

        }
    }

    google.load("visualization", "1", {packages:["treemap"]});
    google.setOnLoadCallback(Treemap.drawChart);

</script>