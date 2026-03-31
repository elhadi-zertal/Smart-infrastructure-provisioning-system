import { useState } from 'react';
import { 
  GraduationCap, Calendar, Target, BookOpen, Server, 
  Container, Cloud, Shield, Activity, Cpu, Network, 
  Code, Zap, Layers, Settings, CheckCircle,
  ChevronRight, ChevronDown, Clock, AlertTriangle,
  BarChart3, Box, Terminal, Globe, HardDrive,
  Monitor, Workflow, GitBranch, Microscope, TrendingUp
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';

interface Phase {
  id: string;
  title: string;
  subtitle: string;
  dateRange: string;
  icon: React.ReactNode;
  color: string;
  status: 'completed' | 'active' | 'upcoming';
  sections: Section[];
}

interface Section {
  title: string;
  icon: React.ReactNode;
  items: string[];
  details?: Record<string, string[]>;
}

const phases: Phase[] = [
  {
    id: 'phase1-learning',
    title: 'Phase 1 — Foundation Learning',
    subtitle: 'Core Technologies & Concepts',
    dateRange: 'July 2025 – November 2025',
    icon: <BookOpen className="w-6 h-6" />,
    color: 'bg-blue-500',
    status: 'completed',
    sections: [
      {
        title: 'Core Learning Topics',
        icon: <Target className="w-5 h-5" />,
        items: ['Ministry Microservices', 'Hospital Microservices']
      },
      {
        title: 'Networking Basics',
        icon: <Network className="w-5 h-5" />,
        items: ['TCP/IP', 'DNS', 'Routing', 'Switching', 'Subnetting', 'VLANs']
      },
      {
        title: 'Security',
        icon: <Shield className="w-5 h-5" />,
        items: ['TLS/SSL', 'Firewalls', 'Authentication', 'HSTS', 'Security Headers']
      },
      {
        title: 'AI Basics',
        icon: <Cpu className="w-5 h-5" />,
        items: ['ML fundamentals', 'Optimization algorithms', 'Metaheuristics']
      },
      {
        title: 'Docker (Containerization)',
        icon: <Container className="w-5 h-5" />,
        items: ['Images', 'Containers', 'Volumes', 'Networks', 'Compose']
      },
      {
        title: 'Proxmox (Virtualization)',
        icon: <Server className="w-5 h-5" />,
        items: ['VMs', 'LXC containers', 'Cluster', 'Storage', 'Backup & snapshots']
      },
      {
        title: 'Kubernetes (K8s)',
        icon: <Cloud className="w-5 h-5" />,
        items: ['Pods', 'Deployments', 'Services', 'ReplicaSets', 'Namespaces', 'HPA', 'kubectl', 'kubeadm']
      },
      {
        title: 'NGINX — Reverse Proxy + Load Balancer',
        icon: <Globe className="w-5 h-5" />,
        items: ['Upstream pools', 'Round-Robin LB', 'TLS Security', 'Location routes', 'Proxy settings']
      },
      {
        title: 'Monitoring Stack',
        icon: <Activity className="w-5 h-5" />,
        items: ['PLG Stack (Prometheus, Loki, Grafana)', 'Alertmanager', 'node_exporter']
      }
    ]
  },
  {
    id: 'phase1-testing',
    title: 'Phase 1 — Production Testing',
    subtitle: 'Cluster Architecture Testing',
    dateRange: 'December 2025',
    icon: <Microscope className="w-6 h-6" />,
    color: 'bg-green-500',
    status: 'completed',
    sections: [
      {
        title: 'Cluster 1 — Basic',
        icon: <Box className="w-5 h-5" />,
        items: ['1 Ministry node + 1 Hospital node', 'No scaling, no redundancy', 'Single node fails → entire service goes DOWN'],
        details: { 'Purpose': ['Baseline prototype / testing environment'] }
      },
      {
        title: 'Cluster 2 — Scaled',
        icon: <Layers className="w-5 h-5" />,
        items: ['kubeadm cluster setup', 'RBAC & namespaces', 'Persistent Volumes (PVC)', 'ConfigMaps / Secrets'],
        details: {
          'Server Configuration': [
            'Ministry server: 2 replicas',
            'Hospital server: 3 replicas',
            '1 replica fails → others continue serving requests'
          ],
          'Purpose': ['Handle more traffic + basic fault tolerance']
        }
      },
      {
        title: 'Cluster 3 — Microservices',
        icon: <GitBranch className="w-5 h-5" />,
        items: ['Microservices architecture', 'Independent deployment', 'Service mesh'],
        details: {
          'Services': ['Soin ×2', 'Notifications ×2', 'Frontend ×2', 'Reception ×2', 'Medicament ×2'],
          'Purpose': ['Modularity', 'Maintainability', 'Independent scaling']
        }
      },
      {
        title: 'Cluster 4 — Hybrid HA (High Availability)',
        icon: <Shield className="w-5 h-5" />,
        items: ['Production-grade HA', 'Fault Tolerance', 'Scalability', 'Container crash → another replica takes over automatically'],
        details: {
          'Result': ['Selected as the deployment strategy for Phase 2']
        }
      }
    ]
  },
  {
    id: 'deepdive',
    title: 'Learning Deepdive',
    subtitle: 'Advanced Topics',
    dateRange: 'January – February 2026',
    icon: <TrendingUp className="w-6 h-6" />,
    color: 'bg-purple-500',
    status: 'completed',
    sections: [
      {
        title: 'Proxmox Advanced',
        icon: <Server className="w-5 h-5" />,
        items: ['16 Physical PCs → Proxmox VE installed on all 16', 'Cluster management', 'Storage: ZFS, LVM', 'SDN: Bridges, VLANs', 'VXLAN zones']
      },
      {
        title: 'Kubernetes Advanced',
        icon: <Cloud className="w-5 h-5" />,
        items: ['HPA / replica scaling', 'K8s Python client', 'Deployment patching API', 'AppsV1Api / CoreV1Api']
      },
      {
        title: 'Docker Advanced',
        icon: <Container className="w-5 h-5" />,
        items: ['Multi-stage builds', 'Docker networking modes', 'Registry & image mgmt', 'Resource limits', 'docker-compose & stacks', 'Container security', 'Docker in Proxmox VMs']
      }
    ]
  },
  {
    id: 'phase2-infra',
    title: 'Phase 2 — Infrastructure',
    subtitle: 'Proxmox + K8s Deployment',
    dateRange: 'February – March 2026',
    icon: <HardDrive className="w-6 h-6" />,
    color: 'bg-orange-500',
    status: 'active',
    sections: [
      {
        title: 'Physical Hardware Setup',
        icon: <Monitor className="w-5 h-5" />,
        items: [
          '16 Physical Machines → Ubuntu Server installed on all 16',
          'Proxmox Cluster formed → 8 Physical Machines joined as cluster nodes',
          'Remaining 8 kept as standby / test machines'
        ]
      },
      {
        title: 'System Flow',
        icon: <Workflow className="w-5 h-5" />,
        items: [
          'Prometheus Alertmanager → webhook → AlterManager API → Proxmox API (VMs/LXC) or Kubernetes API'
        ],
        details: {
          'Scheduler': ['Cluster poll every 10s', 'Warning queue process every 30s (max 1 instance each)']
        }
      },
      {
        title: 'VM Deployment — Hybrid HA Approach',
        icon: <Zap className="w-5 h-5" />,
        items: ['API token auth', 'HA Manager', 'ProxmoxAPI client'],
        details: {
          'Initial VM Sizing': ['3 GB RAM', '1 vCPU', '25 GB Disk']
        }
      }
    ]
  },
  {
    id: 'phase2-api',
    title: 'Phase 2 — AlterManager API',
    subtitle: 'Smart Provisioning Engine',
    dateRange: 'Feb – Mar 2026',
    icon: <Code className="w-6 h-6" />,
    color: 'bg-cyan-500',
    status: 'active',
    sections: [
      {
        title: 'Alert Label Schema',
        icon: <TagIcon className="w-5 h-5" />,
        items: [
          'target_type: "vm" | "lxc" | "k8s"',
          'severity: "warning" | "critical"',
          'type: "cpu" | "memory" | "disk_io" | "network" | "http_5xx"',
          'vmid: "152" (Proxmox VM/LXC ID)',
          'namespace: "default" + deployment: "my-app" (K8s only)'
        ]
      },
      {
        title: 'Alert Thresholds & Auto-Scaling Rules',
        icon: <AlertTriangle className="w-5 h-5" />,
        items: [
          'CPU Usage: >80% → WARNING → queue | >95% → CRITICAL → +1 vCPU (vertical)',
          'RAM Usage: >60% → WARNING → queue | >80% → CRITICAL → +20% RAM (vertical)',
          'Disk I/O (Free): <20% → WARNING → queue | <20% → CRITICAL → +15G disk (vertical)',
          'Network / HTTP Errors: HTTP 5xx >1% → WARNING → queue | HTTP 5xx >5% → CRITICAL → clone VM (horizontal)',
          'Packet Loss >2% → CRITICAL → clone VM (horizontal)'
        ]
      },
      {
        title: 'Mini-Clusters Architecture',
        icon: <Layers className="w-5 h-5" />,
        items: [
          'Divide full cluster into k mini-clusters',
          'Optimizes provisioning algorithm: O(n) → O(n/k)',
          'Organizes VM scaling and new VM creation',
          'New cloned VM joins the same mini-cluster as its parent',
          'Enables localized horizontal scaling decisions'
        ]
      },
      {
        title: 'Scaling Decision Logic',
        icon: <Settings className="w-5 h-5" />,
        items: [
          'WARNING Path → Add to warning_queue (one entry per VM, multi-alert)',
          'CRITICAL Path → Immediate Action',
          'CPU | MEMORY | DISK_IO → Vertical Scale now',
          'NETWORK | HTTP_5XX → Horizontal Scale now',
          'PM resources insufficient → Migrate VM (calls DE-WOA)',
          'Energy cap exceeded → reject, log warning'
        ]
      },
      {
        title: 'Fitness Function — VM Placement Score',
        icon: <BarChart3 className="w-5 h-5" />,
        items: [
          'fitness = √(w₁·(ΔC*)² + w₂·(ΔR*)² + w₃·(ΔIO*)² + w₄·(ΔE*)²)',
          'ΔX = resource_needed_by_VM − resource_available_on_node',
          'ΔX* = (ΔX − mean_ΔX) / σ_ΔX ← z-score normalization',
          'Weights: W_CPU=0.35 | W_RAM=0.35 | W_IO=0.15 | W_E=0.15',
          'Infeasible node (any ΔX > 0) → returns infinity (rejected)'
        ]
      },
      {
        title: 'HYBRID DE-WOA Provisioning Algorithm',
        icon: <Cpu className="w-5 h-5" />,
        items: [
          'Differential Evolution + Whale Optimization Algorithm (adapted)',
          'Randomly sample m PMs from n available online nodes',
          'Map m VMs to m sampled PMs (random permutation)',
          'Calculate total fitness = Σ fitness(vm_i, node_i)',
          'Remap k1=10 times → keep mapping with lowest fitness',
          'Take different random samples (k2=5) → return best overall fitness',
          'Container → VM assignment: Best Fit algorithm'
        ]
      },
      {
        title: 'REST API Endpoints',
        icon: <Terminal className="w-5 h-5" />,
        items: [
          'POST /alertmanager/webhook — Receive Prometheus Alertmanager webhooks',
          'GET /health — Health check + queue size + delta history stats',
          'GET /nodes | /vms | /lxc | /deployments — Live cluster state',
          'PUT /vms/{vmid}/resources — Manual vertical scale (VM)',
          'POST /vms/{vmid}/clone — Manual horizontal scale (VM)',
          'PATCH /deployments/{ns}/{dep}/replicas — K8s manual replica set',
          'GET /warning-queue — View pending DE-WOA items'
        ]
      }
    ]
  },
  {
    id: 'phase3',
    title: 'Phase 3 — AI Integration & Automation',
    subtitle: 'Intelligent Provisioning',
    dateRange: 'April – June 2026',
    icon: <Zap className="w-6 h-6" />,
    color: 'bg-pink-500',
    status: 'upcoming',
    sections: [
      {
        title: 'AI Integration (Planned)',
        icon: <Cpu className="w-5 h-5" />,
        items: [
          'Predictive scaling (ML time-series)',
          'Anomaly detection on metrics',
          'Workload forecasting models',
          'Intelligent threshold auto-tuning',
          'Reinforcement learning for provisioning',
          'Neural network for fitness estimation',
          'Auto-tuning DE-WOA k1, k2 params',
          'Load pattern recognition'
        ]
      },
      {
        title: 'Full Automation (Planned)',
        icon: <Settings className="w-5 h-5" />,
        items: [
          'Zero-touch VM lifecycle management',
          'Auto-remediation of failures',
          'Dynamic mini-cluster rebalancing',
          'Self-healing infrastructure',
          'Intelligent live migration decisions',
          'Automated capacity planning',
          'Pre-emptive / proactive scaling'
        ]
      }
    ]
  }
];

function TagIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 2H2v10l9.29 9.29c.94.94 2.48.94 3.42 0l6.58-6.58c.94-.94.94-2.48 0-3.42L12 2Z" />
      <circle cx="7" cy="7" r="1" />
    </svg>
  );
}

function PhaseCard({ phase, isExpanded, onToggle }: { phase: Phase; isExpanded: boolean; onToggle: () => void }) {
  const statusColors = {
    completed: 'bg-green-500',
    active: 'bg-blue-500',
    upcoming: 'bg-gray-400'
  };

  const statusLabels = {
    completed: 'Completed',
    active: 'In Progress',
    upcoming: 'Upcoming'
  };

  return (
    <Card className="mb-6 overflow-hidden border-l-4 transition-all duration-300 hover:shadow-lg" style={{ borderLeftColor: phase.color.replace('bg-', '') }}>
      <CardHeader 
        className="cursor-pointer bg-gradient-to-r from-gray-50 to-white hover:from-gray-100 transition-colors"
        onClick={onToggle}
      >
        <div className="flex items-start justify-between">
          <div className="flex items-start gap-4">
            <div className={`p-3 rounded-lg ${phase.color} text-white shadow-md`}>
              {phase.icon}
            </div>
            <div className="flex-1">
              <div className="flex items-center gap-3 mb-1">
                <CardTitle className="text-xl font-bold">{phase.title}</CardTitle>
                <Badge className={`${statusColors[phase.status]} text-white`}>
                  {statusLabels[phase.status]}
                </Badge>
              </div>
              <p className="text-sm text-gray-600 font-medium">{phase.subtitle}</p>
              <div className="flex items-center gap-2 mt-2 text-sm text-gray-500">
                <Calendar className="w-4 h-4" />
                <span>{phase.dateRange}</span>
              </div>
            </div>
          </div>
          <button className="p-2 hover:bg-gray-200 rounded-full transition-colors">
            {isExpanded ? <ChevronDown className="w-5 h-5" /> : <ChevronRight className="w-5 h-5" />}
          </button>
        </div>
      </CardHeader>
      
      {isExpanded && (
        <CardContent className="pt-4 pb-6">
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {phase.sections.map((section, idx) => (
              <div key={idx} className="bg-gray-50 rounded-lg p-4 border border-gray-100">
                <div className="flex items-center gap-2 mb-3">
                  <div className="text-gray-700">{section.icon}</div>
                  <h4 className="font-semibold text-gray-800">{section.title}</h4>
                </div>
                <ul className="space-y-2">
                  {section.items.map((item, itemIdx) => (
                    <li key={itemIdx} className="flex items-start gap-2 text-sm text-gray-600">
                      <CheckCircle className="w-4 h-4 text-green-500 mt-0.5 flex-shrink-0" />
                      <span>{item}</span>
                    </li>
                  ))}
                </ul>
                {section.details && (
                  <div className="mt-3 pt-3 border-t border-gray-200">
                    {Object.entries(section.details).map(([key, values]) => (
                      <div key={key} className="mb-2">
                        <span className="text-xs font-semibold text-gray-500 uppercase">{key}:</span>
                        <ul className="mt-1 space-y-1">
                          {values.map((v, i) => (
                            <li key={i} className="text-xs text-gray-500 pl-2 border-l-2 border-gray-300">
                              {v}
                            </li>
                          ))}
                        </ul>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </CardContent>
      )}
    </Card>
  );
}

function NetworkArchitecture() {
  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Network className="w-5 h-5" />
          Virtual Network Architecture
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid gap-4 md:grid-cols-3">
          <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
            <h4 className="font-semibold text-blue-800 mb-2">VLAN 1 — Hospital</h4>
            <p className="text-sm text-blue-600 mb-2">10.10.10.0/24</p>
            <ul className="text-sm text-blue-700 space-y-1">
              <li>• Frontend VMs</li>
              <li>• Reception VMs</li>
              <li>• Medicament VMs</li>
            </ul>
          </div>
          <div className="bg-green-50 rounded-lg p-4 border border-green-200">
            <h4 className="font-semibold text-green-800 mb-2">VLAN 2 — Ministry</h4>
            <p className="text-sm text-green-600 mb-2">10.20.20.0/24</p>
            <ul className="text-sm text-green-700 space-y-1">
              <li>• Soin VMs</li>
              <li>• Notifications VMs</li>
            </ul>
          </div>
          <div className="bg-purple-50 rounded-lg p-4 border border-purple-200">
            <h4 className="font-semibold text-purple-800 mb-2">VLAN 3 — Monitoring</h4>
            <p className="text-sm text-purple-600 mb-2">10.30.30.0/24</p>
            <ul className="text-sm text-purple-700 space-y-1">
              <li>• Prometheus</li>
              <li>• Grafana</li>
              <li>• Loki</li>
              <li>• Alertmanager</li>
            </ul>
          </div>
        </div>
        <div className="mt-4 bg-gray-50 rounded-lg p-4 border border-gray-200">
          <h4 className="font-semibold text-gray-800 mb-2">Physical Network</h4>
          <p className="text-sm text-gray-600">172.25.5.0/24 — All 16 PMs + Router on this physical LAN</p>
          <p className="text-sm text-gray-500 mt-1">Physical switch is UNMANAGED (no VLAN tagging support)</p>
          <p className="text-sm text-gray-500">VXLAN SDN Zones enable virtual L2 segments over unmanaged physical switch</p>
        </div>
      </CardContent>
    </Card>
  );
}

function MonitoringStack() {
  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Activity className="w-5 h-5" />
          Monitoring Stack Comparison
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="plg" className="w-full">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="plg">PLG Stack (Selected)</TabsTrigger>
            <TabsTrigger value="elk">ELK Stack</TabsTrigger>
          </TabsList>
          <TabsContent value="plg" className="mt-4">
            <div className="bg-green-50 rounded-lg p-4 border border-green-200">
              <div className="flex items-center gap-2 mb-3">
                <CheckCircle className="w-5 h-5 text-green-600" />
                <span className="font-semibold text-green-800">Selected Stack</span>
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <div>
                  <h5 className="font-medium text-green-800 mb-2">Components</h5>
                  <ul className="text-sm text-green-700 space-y-1">
                    <li>• Prometheus — metrics scraping</li>
                    <li>• Loki — log aggregation</li>
                    <li>• Grafana — dashboards & visualization</li>
                    <li>• Alertmanager — alert routing</li>
                    <li>• node_exporter on each VM</li>
                  </ul>
                </div>
                <div>
                  <h5 className="font-medium text-green-800 mb-2">Advantages</h5>
                  <ul className="text-sm text-green-700 space-y-1">
                    <li>✓ Cloud-native, lightweight</li>
                    <li>✓ Prometheus-native alerting</li>
                    <li>✓ Integrates with AlterManager API</li>
                    <li>✓ Good cloud-native support</li>
                  </ul>
                </div>
              </div>
            </div>
          </TabsContent>
          <TabsContent value="elk" className="mt-4">
            <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
              <div className="grid gap-3 md:grid-cols-2">
                <div>
                  <h5 className="font-medium text-gray-800 mb-2">Components</h5>
                  <ul className="text-sm text-gray-700 space-y-1">
                    <li>• Elasticsearch — storage/search</li>
                    <li>• Logstash — data pipeline</li>
                    <li>• Kibana — visualization</li>
                    <li>• Filebeat / agents</li>
                  </ul>
                </div>
                <div>
                  <h5 className="font-medium text-gray-800 mb-2">Characteristics</h5>
                  <ul className="text-sm text-gray-700 space-y-1">
                    <li>• Traditional monitoring</li>
                    <li>• Agent-based collection</li>
                    <li>• Complex configuration</li>
                    <li>• Heavy resource usage</li>
                    <li>• Complex scaling</li>
                  </ul>
                </div>
              </div>
            </div>
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
}

function CurrentStatus() {
  return (
    <Card className="mb-6 bg-gradient-to-r from-blue-50 to-indigo-50 border-blue-200">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-blue-800">
          <Clock className="w-5 h-5" />
          Current Status
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-blue-700 font-medium">
          March 2026: Phase 2 Active — Proxmox Infrastructure Deployed, AlterManager API Running, Testing & Improvement Underway
        </p>
      </CardContent>
    </Card>
  );
}

function FinalPresentation() {
  return (
    <Card className="mb-6 bg-gradient-to-r from-amber-50 to-orange-50 border-amber-200">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-amber-800">
          <GraduationCap className="w-5 h-5" />
          Final Presentation
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-amber-700 font-medium text-lg">June 2026 — Bachelor Degree Defense</p>
        <p className="text-amber-600 mt-1">Smart Provisioning System — PFE</p>
      </CardContent>
    </Card>
  );
}

export default function App() {
  const [expandedPhases, setExpandedPhases] = useState<string[]>(['phase2-api', 'phase2-infra']);

  const togglePhase = (phaseId: string) => {
    setExpandedPhases(prev => 
      prev.includes(phaseId) 
        ? prev.filter(id => id !== phaseId)
        : [...prev, phaseId]
    );
  };

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <div className="flex items-center gap-3">
            <div className="p-3 bg-gradient-to-br from-blue-500 to-purple-600 rounded-xl text-white shadow-lg">
              <GraduationCap className="w-8 h-8" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Smart Provisioning System</h1>
              <p className="text-gray-600">PFE Bachelor Degree Project — July 2025 → June 2026</p>
            </div>
          </div>
          <div className="mt-4 flex items-center gap-4">
            <Badge variant="outline" className="text-sm px-3 py-1">
              <Target className="w-4 h-4 mr-1" />
              Goal: Minimize Application Downtime Under High Load
            </Badge>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 py-8">
        <ScrollArea className="h-full">
          {/* Methodology */}
          <Card className="mb-6">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Workflow className="w-5 h-5" />
                Methodology Applied to ALL Phases
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-3">
                {['Design & Conception', 'Implementation', 'Scaling', 'Improvement'].map((step, idx) => (
                  <div key={idx} className="flex items-center gap-2">
                    <div className="w-8 h-8 rounded-full bg-blue-500 text-white flex items-center justify-center font-bold text-sm">
                      {idx + 1}
                    </div>
                    <span className="font-medium text-gray-700">{step}</span>
                    {idx < 3 && <ChevronRight className="w-4 h-4 text-gray-400" />}
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          <CurrentStatus />

          {/* Timeline */}
          <div className="relative">
            <div className="absolute left-4 top-0 bottom-0 w-0.5 bg-gradient-to-b from-blue-500 via-purple-500 to-pink-500" />
            
            <div className="space-y-6 pl-12">
              {phases.map((phase) => (
                <div key={phase.id} className="relative">
                  <div className={`absolute -left-10 top-6 w-5 h-5 rounded-full border-4 border-white shadow-md ${phase.color}`} />
                  <PhaseCard 
                    phase={phase} 
                    isExpanded={expandedPhases.includes(phase.id)}
                    onToggle={() => togglePhase(phase.id)}
                  />
                </div>
              ))}
            </div>
          </div>

          <Separator className="my-8" />

          {/* Network Architecture */}
          <NetworkArchitecture />

          {/* Monitoring Stack */}
          <MonitoringStack />

          {/* Final Presentation */}
          <FinalPresentation />

          {/* Footer */}
          <footer className="text-center text-gray-500 text-sm py-6">
            <p>Smart Provisioning System — PFE Bachelor Degree Project</p>
            <p className="mt-1">Built with React + TypeScript + Tailwind CSS</p>
          </footer>
        </ScrollArea>
      </main>
    </div>
  );
}
