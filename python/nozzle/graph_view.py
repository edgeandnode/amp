from nozzle.registry_manager import RegistryManager
from nozzle.table_registry import TableRegistry
from nozzle.contract_registry import ContractRegistry
from nozzle.event_registry import EventRegistry
from nozzle.event_parameter_registry import EventParameterRegistry
from nozzle.view import View
import json
from pathlib import Path
import webbrowser
import sys
import inspect
import importlib

def prepare_network_data():
    nodes = []
    edges = []

    # Add tables as nodes
    for table_name in TableRegistry.list_tables():
        table = TableRegistry.get_table(table_name)
        nodes.append({
            "id": f"table_{table_name}",
            "label": table_name,
            "group": "table",
            "description": table.description,
            "attributes": {
                "schema": [{"name": field.name, "type": str(field.type)} for field in table.schema],
                "parquet_files": [str(f) for f in table.parquet_files],
                "min_block": table.min_block,
                "max_block": table.max_block
            }
        })

    # Add contracts as nodes
    for contract_name in dir(ContractRegistry):
        if not contract_name.startswith("__"):
            contract = getattr(ContractRegistry, contract_name)
            nodes.append({
                "id": f"contract_{contract_name}",
                "label": contract_name,
                "group": "contract",
                "description": f"Contract: {contract_name}",
                "attributes": {
                    "address": str(contract.address),
                    "chain": str(contract.chain)
                }
            })

    # Add events as nodes and create edges to contracts
    for contract_name in dir(EventRegistry):
        if not contract_name.startswith("__"):
            contract_events = getattr(EventRegistry, contract_name)
            for event_name in dir(contract_events):
                if not event_name.startswith("__"):
                    event = getattr(contract_events, event_name)
                    nodes.append({
                        "id": f"event_{contract_name}_{event_name}",
                        "label": f"{event_name}",
                        "group": "event",
                        "description": event.description,
                        "attributes": {
                            "signature": event.signature,
                            "contract": event.contract.name
                        }
                    })
                    edges.append({
                        "from": f"contract_{contract_name}",
                        "to": f"event_{contract_name}_{event_name}"
                    })

                    # Add event parameters as nodes and create edges to events
                    for param_name in dir(event.parameters):
                        if not param_name.startswith("__"):
                            param = getattr(event.parameters, param_name)
                            nodes.append({
                                "id": f"param_{contract_name}_{event_name}_{param_name}",
                                "label": param_name,
                                "group": "parameter",
                                "description": param.description,
                                "attributes": {
                                    "abi_type": param.abi_type,
                                    "indexed": param.indexed
                                }
                            })
                            edges.append({
                                "from": f"event_{contract_name}_{event_name}",
                                "to": f"param_{contract_name}_{event_name}_{param_name}"
                            })

   # Dynamically import all Python files in the 'examples' directory
    examples_dir = Path(__file__).parent.parent / 'examples'
    for file in examples_dir.glob('*.py'):
        if file.name != '__init__.py':
            module_name = f'examples.{file.stem}'
            importlib.import_module(module_name)

    # Find all View subclasses
    view_classes = [cls for cls in View.__subclasses__()]

    # Add edges between tables based on input tables of view subclasses
    for view_class in view_classes:
        try:
            view_instance = view_class(start_block=0, end_block=1)  # Create an instance with dummy block range
            for input_table in view_instance.input_tables:
                edges.append({
                    "from": f"table_{view_class.__name__}",
                    "to": f"table_{input_table.name}",
                    "label": "input"
                })
        except Exception as e:
            print(f"Error initializing {view_class.__name__}: {str(e)}")

    # Add edges between event preprocessing tables and their corresponding events
    for table_name in TableRegistry.list_tables():
        if table_name.startswith("preprocessed_event_"):
            parts = table_name.split("_")
            if len(parts) >= 4:
                contract_name = parts[2]
                event_name = parts[3]
                event_id = f"event_{contract_name}_{event_name}"
                for node in nodes:
                    if node['id'].lower() == event_id.lower():
                        edges.append({
                            "from": f"table_{table_name}",
                            "to": node['id'],
                            "label": "preprocessed"
                        })

    return {"nodes": nodes, "edges": edges}

def generate_html(data):
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Local Nozzle Data Catalog</title>
        <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
        <style>
            body {{
                margin: 0;
                padding: 0;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                overflow: hidden;
            }}
            #mynetwork {{
                width: 100%;
                height: 100vh;
                border: 1px solid #00ff00;
                box-shadow: 0 0 20px rgba(0, 255, 0, 0.3);
            }}
            #node-info {{
                position: fixed;
                bottom: 0;
                left: 50%;
                transform: translateX(-50%);
                width: 80%;
                max-width: 800px;
                height: 35vh;
                overflow-y: auto;
                background-color: rgba(255, 255, 255, 0.9);
                color: #000000;
                padding: 20px;
                display: none;
                box-shadow: 0 -5px 10px rgba(0, 0, 0, 0.3);
                border-radius: 10px 10px 0 0;
            }}
            .group-label {{
                font-size: 1.2em;
                color: #00aa00;
                text-transform: uppercase;
                margin-bottom: 10px;
            }}
            #sidebar {{
                position: fixed;
                top: 0;
                left: -300px;
                width: 300px;
                height: 100vh;
                background-color: rgba(255, 255, 255, 0.9);
                transition: left 0.3s ease-in-out;
                overflow-y: auto;
                z-index: 1000;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.3);
                padding: 20px;
            }}
            #sidebar.open {{
                left: 0;
            }}
            #sidebar-toggle {{
                position: fixed;
                top: 10px;
                left: 10px;
                z-index: 1001;
                background-color: #591b9b;
                color: white;
                border: none;
                padding: 10px 15px;
                cursor: pointer;
                font-size: 16px;
                border-radius: 5px;
            }}
            #sidebar.open + #sidebar-toggle {{
                left: 310px;
            }}
            .tree-list {{
                list-style-type: none;
                padding-left: 20px;
            }}
            .tree-item {{
                cursor: pointer;
                color: #000000;
                padding: 5px 0;
            }}
            .tree-item:hover {{
                background-color: #f0f0f0;
            }}
            .tree-toggle {{
                cursor: pointer;
                user-select: none;
            }}
            .tree-toggle::before {{
                content: '▶';
                display: inline-block;
                margin-right: 6px;
                transition: transform 0.3s;
            }}
            .tree-toggle.open::before {{
                transform: rotate(90deg);
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        <div id="mynetwork"></div>
        <div id="node-info"></div>
        <button id="sidebar-toggle">☰</button>
        <div id="sidebar">
            <br><br>
            <h3>Local Nozzle Data Catalog</h3>
            <div id="tree-view"></div>
        </div>

        <script type="text/javascript">
            var nodes = new vis.DataSet({json.dumps(data['nodes'])});
            var edges = new vis.DataSet({json.dumps(data['edges'])});

            var container = document.getElementById('mynetwork');
            var data = {{
                nodes: nodes,
                edges: edges
            }};
            var options = {{
                nodes: {{
                    shape: 'dot',
                    size: 20,
                    font: {{
                        face: 'Segoe UI',
                        size: 18,
                        color: '#000000',
                    }},
                    borderWidth: 2,
                    shadow: true,
                    chosen: {{
                        node: (values, id, selected, hovering) => {{
                            values.size = selected ? 30 : 20;
                            values.borderWidth = selected ? 3 : 2;
                            if (selected) {{
                                values.color = brightenColor(values.color, 40);
                            }}
                        }}
                    }}
                }},
                edges: {{
                    width: 2,
                    color: {{color: '#2c3e50', highlight: '#64ffda'}},
                    arrows: {{to: {{enabled: true, scaleFactor: 1, type: 'arrow'}}}},
                    font: {{
                        face: 'Segoe UI',
                        size: 14,
                        color: '#000000'
                    }}
                }},
                groups: {{
                    table: {{color: '#4a69bd', borderWidth: 2, borderWidthSelected: 4}},
                    contract: {{color: '#60a3bc', borderWidth: 2, borderWidthSelected: 4}},
                    event: {{color: '#82ccdd', borderWidth: 2, borderWidthSelected: 4}},
                    parameter: {{color: '#b8e994', borderWidth: 2, borderWidthSelected: 4}}
                }},
                layout: {{
                    hierarchical: {{
                        direction: 'UD',
                        sortMethod: 'directed',
                        levelSeparation: 200,
                        nodeSpacing: 250
                    }}
                }},
                physics: false
            }};

            function brightenColor(hex, percent) {{
                // ... (brightenColor function remains the same) ...
            }}

            var network = new vis.Network(container, data, options);

            network.on("hoverNode", function (params) {{
                var node = nodes.get(params.node);
                if (node.description) {{
                    network.canvas.body.container.title = node.description;
                }}
            }});

            function showNodeInfo(nodeId) {{
                var node = nodes.get(nodeId);
                var nodeInfo = document.getElementById('node-info');
                var content = `<div class="group-label">${{node.group}}</div>
                               <h2>${{node.label}}</h2>
                               <p>${{node.description}}</p>`;
                
                if (node.group === 'table') {{
                    content += `<h3>Schema:</h3>
                                <table>
                                    <tr><th>Name</th><th>Type</th></tr>
                                    ${{node.attributes.schema.map(field => `<tr><td>${{field.name}}</td><td>${{field.type}}</td></tr>`).join('')}}
                                </table>
                                <h3>Parquet Files:</h3>
                                <ul>
                                    ${{node.attributes.parquet_files.map(file => `<li>${{file}}</li>`).join('')}}
                                </ul>
                                <p>Min Block: ${{node.attributes.min_block}}</p>
                                <p>Max Block: ${{node.attributes.max_block}}</p>`;
                }} else {{
                    content += `<pre>${{JSON.stringify(node.attributes, null, 2)}}</pre>`;
                }}
                
                nodeInfo.innerHTML = content;
                nodeInfo.style.display = 'block';
            }}

            network.on("click", function (params) {{
                if (params.nodes.length > 0) {{
                    showNodeInfo(params.nodes[0]);
                }} else {{
                    document.getElementById('node-info').style.display = 'none';
                }}
            }});

            function generateTreeView() {{
                const treeView = document.getElementById('tree-view');
                const nodeMap = new Map();

                // Create a map of all nodes
                nodes.forEach(node => {{
                    nodeMap.set(node.id, {{ ...node, children: [] }});
                }});

                // Build the tree structure
                edges.forEach(edge => {{
                    const parent = nodeMap.get(edge.from);
                    const child = nodeMap.get(edge.to);
                    if (parent && child) {{
                        parent.children.push(child);
                    }}
                }});

                function generateTreeHTML(node) {{
                    let html = `<li>`;
                    if (node.children.length > 0) {{
                        html += `<span class="tree-toggle">${{node.label}}</span>`;
                        html += `<ul class="tree-list" style="display: none;">`;
                        node.children.forEach(child => {{
                            html += generateTreeHTML(child);
                        }});
                        html += `</ul>`;
                    }} else {{
                        html += `<span class="tree-item" data-node-id="${{node.id}}">${{node.label}}</span>`;
                    }}
                    html += `</li>`;
                    return html;
                }}

                const groupedNodes = {{
                    tables: [],
                    contracts: [],
                    others: []
                }};

                nodeMap.forEach(node => {{
                    if (node.group === 'table') {{
                        groupedNodes.tables.push(node);
                    }} else if (node.group === 'contract') {{
                        groupedNodes.contracts.push(node);
                    }} else if (!edges.get({{to: node.id}})) {{
                        groupedNodes.others.push(node);
                    }}
                }});

                let treeHtml = '<ul class="tree-list">';
                
                treeHtml += '<li><span class="tree-toggle">Tables</span><ul class="tree-list" style="display: none;">';
                groupedNodes.tables.forEach(node => {{
                    treeHtml += generateTreeHTML(node);
                 }});
                treeHtml += '</ul></li>';

                treeHtml += '<li><span class="tree-toggle">Contracts</span><ul class="tree-list" style="display: none;">';
                groupedNodes.contracts.forEach(node => {{
                    treeHtml += generateTreeHTML(node);
                }});
                treeHtml += '</ul></li>';

                if (groupedNodes.others.length > 0) {{
                    treeHtml += '<li><span class="tree-toggle">Others</span><ul class="tree-list" style="display: none;">';
                    groupedNodes.others.forEach(node => {{
                        treeHtml += generateTreeHTML(node);
                    }});
                    treeHtml += '</ul></li>';
                }}

                treeHtml += '</ul>';

                treeView.innerHTML = treeHtml;

                document.querySelectorAll('.tree-item').forEach(item => {{
                    item.addEventListener('click', function(event) {{
                        event.stopPropagation();
                        const nodeId = this.getAttribute('data-node-id');
                        network.selectNodes([nodeId]);
                        network.focus(nodeId, {{
                            scale: 1.5,
                            animation: true
                        }});
                        showNodeInfo(nodeId);
                    }});
                }});

                document.querySelectorAll('.tree-toggle').forEach(toggle => {{
                    toggle.addEventListener('click', function(event) {{
                        event.stopPropagation();
                        this.classList.toggle('open');
                        const subList = this.nextElementSibling;
                        if (subList) {{
                            subList.style.display = subList.style.display === 'none' ? 'block' : 'none';
                        }}
                    }});
                }});
            }}
            

            document.getElementById('sidebar-toggle').addEventListener('click', function() {{
                document.getElementById('sidebar').classList.toggle('open');
                this.classList.toggle('open');
            }});

            generateTreeView();
        </script>
    </body>
    </html>
    """
    return html_content

def launch_visualization():
    data = prepare_network_data()
    html_content = generate_html(data)
    
    # Write the HTML content to a file
    output_path = Path.cwd() / "data_catalog_visualization.html"
    with output_path.open("w") as f:
        f.write(html_content)
    
    # Open the HTML file in the default web browser
    webbrowser.open(output_path.as_uri())

if __name__ == "__main__":
    launch_visualization()