import graphviz

# ==========================================
# 1. VM vs Container Architecture Diagram
# ==========================================
d1 = graphviz.Digraph('arch_vm_container', format='png')
d1.attr(rankdir='LR', nodesep='1')
d1.node('VM', '''<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="8">
  <TR><TD BGCOLOR="#bbdefb">App A</TD><TD BGCOLOR="#bbdefb">App B</TD><TD BGCOLOR="#bbdefb">App C</TD></TR>
  <TR><TD>Bins/Libs</TD><TD>Bins/Libs</TD><TD>Bins/Libs</TD></TR>
  <TR><TD>Guest OS</TD><TD>Guest OS</TD><TD>Guest OS</TD></TR>
  <TR><TD COLSPAN="3" BGCOLOR="#e0e0e0">Hypervisor</TD></TR>
  <TR><TD COLSPAN="3" BGCOLOR="#e0e0e0">Host OS</TD></TR>
  <TR><TD COLSPAN="3" BGCOLOR="#757575"><FONT COLOR="white">Hardware Infrastructure</FONT></TD></TR>
</TABLE>>''', shape='none')
d1.node('Container', '''<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="8">
  <TR><TD BGCOLOR="#c8e6c9">App A</TD><TD BGCOLOR="#c8e6c9">App B</TD><TD BGCOLOR="#c8e6c9">App C</TD></TR>
  <TR><TD>Bins/Libs</TD><TD>Bins/Libs</TD><TD>Bins/Libs</TD></TR>
  <TR><TD COLSPAN="3" BGCOLOR="#e0e0e0">Container Engine (e.g., Docker)</TD></TR>
  <TR><TD COLSPAN="3" BGCOLOR="#e0e0e0">Host OS</TD></TR>
  <TR><TD COLSPAN="3" BGCOLOR="#757575"><FONT COLOR="white">Hardware Infrastructure</FONT></TD></TR>
</TABLE>>''', shape='none')
d1.render(cleanup=True)

# ==========================================
# 2. Full General Cluster Architecture
# ==========================================
full = graphviz.Digraph('cluster_full', format='png')
full.attr(rankdir='TB', compound='true', nodesep='0.8', ranksep='0.8')
with full.subgraph(name='cluster_L4') as l4:
    l4.attr(label='Layer 4: Monitoring & AI', style='filled', fillcolor='#e3f2fd', color='blue')
    l4.node('AI', 'Cognitive Agents\n(Planner & Critic)', shape='box', style='filled', fillcolor='#bbdefb')
    l4.node('Alert', 'Alert Manager\n(Rules & Escalation)', shape='box', style='filled', fillcolor='#bbdefb')
    l4.node('Tele', 'Telemetry Aggregator', shape='cylinder', style='filled', fillcolor='#bbdefb')
    l4.edge('Tele', 'Alert')
    l4.edge('Alert', 'AI')
with full.subgraph(name='cluster_L3') as l3:
    l3.attr(label='Layer 3: Containerisation', style='filled', fillcolor='#e8f5e9', color='green')
    l3.node('Scheduler', 'Best-Fit\nScheduler', shape='hexagon', style='filled', fillcolor='#c8e6c9')
    l3.node('C1', 'Microservice\nContainers', shape='box3d', style='filled', fillcolor='#c8e6c9')
    l3.edge('Scheduler', 'C1', label=' Places')
with full.subgraph(name='cluster_L2') as l2:
    l2.attr(label='Layer 2: Virtualisation', style='filled', fillcolor='#fffde7', color='#fbc02d')
    l2.node('WOADE', 'WOA-DE\nProvisioning', shape='hexagon', style='filled', fillcolor='#fff9c4')
    l2.node('MiniC', 'Logical Mini-Clusters\n(VM Fleet)', shape='folder', style='filled', fillcolor='#fff9c4')
    l2.edge('WOADE', 'MiniC', label=' Provisions')
with full.subgraph(name='cluster_L1') as l1:
    l1.attr(label='Layer 1: Physical Infrastructure', style='filled', fillcolor='#f5f5f5', color='black')
    l1.node('Net', 'Redundant Network\n(Routers, Firewalls, Switches)', shape='box', style='filled', fillcolor='#e0e0e0')
    l1.node('Servers', 'Physical Servers\n(Failure Domains)', shape='box', style='filled', fillcolor='#e0e0e0')
    l1.edge('Net', 'Servers', label=' Connects')
full.edge('AI', 'Scheduler', label=' Scale up/down', ltail='cluster_L4', lhead='cluster_L3', style='dashed')
full.edge('Scheduler', 'WOADE', label=' Triggers VM creation', style='dashed')
full.edge('C1', 'MiniC', label=' Hosted on')
full.edge('MiniC', 'Servers', label=' Abstracted from')
full.edge('Servers', 'Tele', label=' Metrics (Push/Pull)', style='dotted', constraint='false')
full.render(cleanup=True)

# ==========================================
# 3. Layer 1: Physical Layer Diagram
# ==========================================
l1_diag = graphviz.Digraph('layer1_physical', format='png')
l1_diag.attr(rankdir='TB', nodesep='0.6', ranksep='0.7')
l1_diag.node('Inet', 'External Internet', shape='ellipse', style='filled', fillcolor='#e0f7fa')
l1_diag.node('Router1', 'Edge Router A', shape='box')
l1_diag.node('Router2', 'Edge Router B', shape='box')
l1_diag.node('FW1', 'Hardware Firewall A', shape='box')
l1_diag.node('FW2', 'Hardware Firewall B', shape='box')
l1_diag.node('Core', 'Core Switch Fabric', shape='hexagon', style='filled', fillcolor='#e0e0e0')
with l1_diag.subgraph(name='cluster_fd1') as fd1:
    fd1.attr(label='Failure Domain 1', style='dashed')
    fd1.node('TOR1', 'Top-of-Rack Switch 1')
    fd1.node('S1', 'Server Node 1')
    fd1.node('S2', 'Server Node 2')
    fd1.edge('TOR1', 'S1')
    fd1.edge('TOR1', 'S2')
with l1_diag.subgraph(name='cluster_fd2') as fd2:
    fd2.attr(label='Failure Domain 2', style='dashed')
    fd2.node('TOR2', 'Top-of-Rack Switch 2')
    fd2.node('S3', 'Server Node 3')
    fd2.node('S4', 'Server Node 4')
    fd2.edge('TOR2', 'S3')
    fd2.edge('TOR2', 'S4')
l1_diag.edge('Inet', 'Router1')
l1_diag.edge('Inet', 'Router2')
l1_diag.edge('Router1', 'FW1')
l1_diag.edge('Router2', 'FW2')
l1_diag.edge('FW1', 'Core')
l1_diag.edge('FW2', 'Core')
l1_diag.edge('Core', 'TOR1')
l1_diag.edge('Core', 'TOR2')
l1_diag.render(cleanup=True)

# ==========================================
# 4. Layer 2: Virtual Layer Diagram
# ==========================================
l2_diag = graphviz.Digraph('layer2_virtual', format='png')
l2_diag.attr(rankdir='LR', nodesep='0.5', ranksep='0.8')
l2_diag.node('Algo', 'WOA-DE\nAlgorithm', shape='hexagon', style='filled', fillcolor='#fff9c4')
l2_diag.node('Fit', 'Fitness Function\n(CPU, RAM, I/O, Energy)', shape='note', style='filled', fillcolor='#fff9c4')
l2_diag.node('VMS', 'VM Scaling\n(Horizontal / Vertical)', shape='box', style='rounded,filled', fillcolor='#fff9c4')
with l2_diag.subgraph(name='cluster_mc1') as mc1:
    mc1.attr(label='Mini-Cluster K1 (Partition)', style='filled', fillcolor='#fcf8e3')
    mc1.node('VM1', 'VM 1\n(App X)', shape='box3d')
    mc1.node('VM2', 'VM 2\n(App X)', shape='box3d')
with l2_diag.subgraph(name='cluster_mc2') as mc2:
    mc2.attr(label='Mini-Cluster K2 (Partition)', style='filled', fillcolor='#fcf8e3')
    mc2.node('VM3', 'VM 3\n(App Y)', shape='box3d')
l2_diag.node('Phys', 'Physical Servers', shape='cylinder', style='filled', fillcolor='#eeeeee')
l2_diag.edge('Fit', 'Algo', label=' Evaluates candidates')
l2_diag.edge('VMS', 'Algo', label=' Triggers')
l2_diag.edge('Algo', 'VM1', label=' Provisions')
l2_diag.edge('Algo', 'VM3', label=' Provisions')
l2_diag.edge('VM1', 'Phys', label=' Placed on')
l2_diag.edge('VM2', 'Phys', label=' Placed on')
l2_diag.edge('VM3', 'Phys', label=' Placed on')
l2_diag.render(cleanup=True)

# ==========================================
# 5. Virtual Network Diagram (Abstracted)
# ==========================================
vnet = graphviz.Digraph('virtual_network', format='png')
vnet.attr(rankdir='TB', nodesep='0.8', ranksep='0.6')
vnet.node('Ext', 'External Network', shape='ellipse', style='filled', fillcolor='#eeeeee')
vnet.node('LB', 'Load Balancer\n(L4/L7 Ingress)', shape='box', style='filled', fillcolor='#e1bee7', color='#8e24aa')
vnet.node('Router', 'Virtual Router (Gateway)\n(Firewall, NAT, DHCP, DNS)', shape='box', style='filled', fillcolor='#ffcdd2', color='#e53935')
with vnet.subgraph(name='cluster_segments') as seg:
    seg.attr(style='invis') 
    seg.node('VLAN1', 'Workload Segment 1\n(VLAN 1)', shape='folder', style='filled', fillcolor='#bbdefb', color='#1e88e5')
    seg.node('Mon', 'Monitoring Segment\n(Observability & Scraping)', shape='folder', style='filled', fillcolor='#ffe082', color='#ffb300')
    seg.node('VLAN2', 'Workload Segment 2\n(VLAN 2)', shape='folder', style='filled', fillcolor='#c8e6c9', color='#43a047')
vnet.edge('Ext', 'LB', label=' ingress')
vnet.edge('LB', 'Router', label=' forwarded traffic')
vnet.edge('Router', 'VLAN1', label=' GW', color='#1e88e5', fontcolor='#1e88e5')
vnet.edge('Router', 'Mon', label=' GW', color='#ffb300', fontcolor='#ffb300')
vnet.edge('Router', 'VLAN2', label=' GW', color='#43a047', fontcolor='#43a047')
vnet.render(cleanup=True)

# ==========================================
# 6. Layer 3: Containerisation Diagram
# ==========================================
l3_diag = graphviz.Digraph('layer3_container', format='png')
l3_diag.attr(rankdir='TB', nodesep='0.6', ranksep='0.7')
l3_diag.node('NewReq', 'New Microservice\nDeployment', shape='ellipse', style='filled', fillcolor='#e8f5e9')
l3_diag.node('BestFit', 'Best Fit Strategy\n(Minimises Resource Fragmentation)', shape='hexagon', style='filled', fillcolor='#c8e6c9')
with l3_diag.subgraph(name='cluster_vmA') as vmA:
    vmA.attr(label='Virtual Machine A\n(High Load)', style='filled', fillcolor='#ffffff', color='red')
    vmA.node('C1', 'Container\n(Service 1)')
    vmA.node('C2', 'Container\n(Service 2)')
with l3_diag.subgraph(name='cluster_vmB') as vmB:
    vmB.attr(label='Virtual Machine B\n(Spare Capacity)', style='filled', fillcolor='#ffffff', color='green')
    vmB.node('C3', 'Container\n(Service 1 Replica)')
l3_diag.edge('NewReq', 'BestFit')
l3_diag.edge('BestFit', 'C3', label=' Schedules to VM with\nleast remaining capacity')
l3_diag.edge('C1', 'C2', label=' Localhost IPC', style='dashed', constraint='false')
l3_diag.render(cleanup=True)

# ==========================================
# 7. Layer 4: Monitoring, AI & Security Diagram
# ==========================================
l4_diag = graphviz.Digraph('layer4_monitoring', format='png')
l4_diag.attr(rankdir='LR', nodesep='0.4', ranksep='0.8')
l4_diag.node('Agents', 'Pull-based\nTelemetry Agents', shape='cds', style='filled', fillcolor='#eeeeee')
l4_diag.node('TSDB', 'Time-Series Data\n(Metrics & Logs)', shape='cylinder', style='filled', fillcolor='#bbdefb')
with l4_diag.subgraph(name='cluster_analytics') as ana:
    ana.attr(label='Decision & AI Core', style='filled', fillcolor='#e3f2fd')
    ana.node('AlertMgr', 'Alert Manager\n(Grouping/Inhibition)', shape='box')
    ana.node('Pred', 'Predictive Resource\nAllocation Model', shape='box')
    ana.node('Cognitive', 'Dual-Agent System\n(Planner & Critic)', shape='box3d', style='filled', fillcolor='#90caf9')
l4_diag.node('Sec', 'Security Monitoring\n(Anomaly & Signatures)', shape='octagon', style='filled', fillcolor='#ffcdd2')
l4_diag.node('Exec', 'Execution Layer\n(Signed Proposals)', shape='component', style='filled', fillcolor='#e0e0e0')
l4_diag.node('Audit', 'Immutable\nAudit Log', shape='note', style='filled', fillcolor='#b2dfdb')
l4_diag.edge('Agents', 'TSDB', label=' Scrape')
l4_diag.edge('TSDB', 'AlertMgr')
l4_diag.edge('TSDB', 'Pred', label=' Train/Predict')
l4_diag.edge('TSDB', 'Sec', label=' Evaluate')
l4_diag.edge('AlertMgr', 'Cognitive', label=' Anomalies')
l4_diag.edge('Pred', 'Cognitive', label=' Dynamic Thresholds')
l4_diag.edge('Cognitive', 'Exec', label=' Approved Actions')
l4_diag.edge('Sec', 'Exec', label=' Containment Actions')
l4_diag.edge('Exec', 'Audit', label=' Records')
l4_diag.render(cleanup=True)

# ==========================================
# 8. WOA-DE Flowchart (FIXED: SEQUENTIAL)
# ==========================================
f = graphviz.Digraph('flowchart', format='png')
f.attr(rankdir='TB', nodesep='0.8', ranksep='0.6')

f.node('Start', 'Start', shape='oval', style='filled', fillcolor='#e0e0e0')
f.node('Init', 'Initialize Population X\nSet Global Best X*', shape='box')
f.node('LoopCond', 'Is t < T?', shape='diamond', style='filled', fillcolor='#fff9c4')
f.node('Params', 'Update Control Parameters:\na, A, C, p', shape='box')
f.node('Split', 'Partition Population into\nWOA Subset & DE Subset', shape='box', style='filled', fillcolor='#e1bee7')

# WOA Nodes
f.node('WOACond', 'p < 0.5?', shape='diamond')
f.node('WOA_A', '|A| < 1?', shape='diamond')
f.node('WOA_Exploit', 'Exploitation:\nX = X* - A.D', shape='box')
f.node('WOA_Explore', 'Exploration:\nX = X_rand - A.D', shape='box')
f.node('WOA_Spiral', 'Spiral Update:\nX = D.e^{bl}cos(2πl) + X*', shape='box')
f.node('WOA_Refresh', 'Refresh Best X*\n(from WOA subset)', shape='box', style='filled', fillcolor='#c8e6c9')

# DE Nodes
f.node('DE_Mut', 'Mutation:\nV = X* + F(X_r1 - X_r2)', shape='box')
f.node('DE_Cross', 'Crossover:\nU = V (if rand ≤ CR) else X', shape='box')
f.node('DE_Sel', 'Selection:\nReplace if f(U) < f(X)', shape='box')
f.node('DE_Refresh', 'Refresh Best X*\n(from DE subset)', shape='box', style='filled', fillcolor='#c8e6c9')

f.node('Increment', 't = t + 1', shape='box')
f.node('End', 'Return Best Placement X*', shape='oval', style='filled', fillcolor='#e0e0e0')

f.edge('Start', 'Init')
f.edge('Init', 'LoopCond')
f.edge('LoopCond', 'Params', label=' Yes')
f.edge('Params', 'Split')

# Sequential execution edges
f.edge('Split', 'WOACond', label=' 1. Process WOA Subset')
f.edge('WOACond', 'WOA_A', label=' Yes')
f.edge('WOACond', 'WOA_Spiral', label=' No')
f.edge('WOA_A', 'WOA_Exploit', label=' Yes')
f.edge('WOA_A', 'WOA_Explore', label=' No')

f.edge('WOA_Exploit', 'WOA_Refresh')
f.edge('WOA_Explore', 'WOA_Refresh')
f.edge('WOA_Spiral', 'WOA_Refresh')

# Pass the updated X* to the DE block
f.edge('WOA_Refresh', 'DE_Mut', label=' 2. Process DE Subset\n(Using updated X*)')

f.edge('DE_Mut', 'DE_Cross')
f.edge('DE_Cross', 'DE_Sel')
f.edge('DE_Sel', 'DE_Refresh')

f.edge('DE_Refresh', 'Increment')
f.edge('Increment', 'LoopCond')
f.edge('LoopCond', 'End', label=' No')
f.render(cleanup=True)

# ==========================================
# 9. Cognitive AI Agents & Security Matrix
# ==========================================
a = graphviz.Digraph('ai_security', format='png')
a.attr(rankdir='LR', nodesep='0.5', ranksep='0.6')
a.node('Telemetry', 'Cluster\nTelemetry', shape='cylinder', style='filled', fillcolor='#eeeeee')
a.node('Planner', 'Planner Agent\n(Proposes Action)', shape='box', style='filled', fillcolor='#bbdefb')
a.node('Critic', 'Critic Agent\n(Validates & Escalates)', shape='box', style='filled', fillcolor='#ffe082')
a.node('LowRisk', 'Low/Medium\nRisk Action', shape='box', style='rounded,filled', fillcolor='#c8e6c9')
a.node('HighRisk', 'High Risk Action', shape='box', style='rounded,filled', fillcolor='#ffcdd2')
a.node('Admin', 'Administrator\n(Cryptographic Auth)', shape='ellipse', style='filled', fillcolor='#e1bee7')
a.node('Exec', 'Execution Interface\n(Provisioning/Scaling)', shape='box', style='filled', fillcolor='#b2dfdb')
a.edge('Telemetry', 'Planner')
a.edge('Planner', 'Critic', label=' Proposal &\nInitial Risk')
a.edge('Critic', 'LowRisk', label=' Approved')
a.edge('Critic', 'HighRisk', label=' Escalated')
a.edge('LowRisk', 'Exec', label=' Auto-execute')
a.edge('HighRisk', 'Admin', label=' Wait for Auth\n(TTL Queue)')
a.edge('Admin', 'Exec', label=' Approve')
a.render(cleanup=True)

print("All diagrams generated successfully with the corrected sequential WOA-DE Flowchart!")
