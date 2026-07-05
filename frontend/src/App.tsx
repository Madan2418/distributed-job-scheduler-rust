import {
  Activity,
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  Clock3,
  LogOut,
  Pause,
  Play,
  Plus,
  RefreshCw,
  Send,
  Server,
  ShieldCheck,
  Workflow
} from "lucide-react";
import { FormEvent, useEffect, useMemo, useState } from "react";

type Project = {
  id: string;
  name: string;
  slug: string;
  description?: string;
};

type Queue = {
  id: string;
  project_id: string;
  name: string;
  description?: string;
  concurrency_limit: number;
  is_paused: boolean;
};

type QueueStats = {
  queue_id: string;
  queue_name: string;
  queued_count: number;
  running_count: number;
  failed_count: number;
  completed_count: number;
  dlq_count: number;
};

type DlqEntry = {
  id: string;
  job_id: string;
  queue_id: string;
  job_name: string;
  last_error?: string;
  attempt_count: number;
  moved_to_dlq_at: string;
  manually_retried_at?: string;
};

type Metrics = Record<string, number>;

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";

function slugify(value: string) {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

async function parseResponse<T>(response: Response): Promise<T> {
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data?.error?.message ?? "Request failed");
  }
  return data as T;
}

export function App() {
  const [token, setToken] = useState(() => localStorage.getItem("scheduler_token") ?? "");
  const [mode, setMode] = useState<"login" | "register">("login");
  const [email, setEmail] = useState("operator@example.com");
  const [password, setPassword] = useState("password123");
  const [displayName, setDisplayName] = useState("Operator");
  const [projects, setProjects] = useState<Project[]>([]);
  const [selectedProjectId, setSelectedProjectId] = useState("");
  const [queues, setQueues] = useState<Queue[]>([]);
  const [selectedQueueId, setSelectedQueueId] = useState("");
  const [stats, setStats] = useState<QueueStats | null>(null);
  const [metrics, setMetrics] = useState<Metrics>({});
  const [dlq, setDlq] = useState<DlqEntry[]>([]);
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);

  const selectedProject = projects.find((project) => project.id === selectedProjectId);
  const selectedQueue = queues.find((queue) => queue.id === selectedQueueId);

  const authHeaders = useMemo(
    () => ({
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`
    }),
    [token]
  );

  async function request<T>(path: string, options: RequestInit = {}) {
    return parseResponse<T>(
      await fetch(`${API_BASE}${path}`, {
        ...options,
        headers: {
          ...(token ? authHeaders : { "Content-Type": "application/json" }),
          ...(options.headers ?? {})
        }
      })
    );
  }

  async function loadProjects() {
    const data = await request<{ projects: Project[] }>("/v1/projects");
    setProjects(data.projects);
    setSelectedProjectId((current) => current || data.projects[0]?.id || "");
  }

  async function loadQueues(projectId: string) {
    if (!projectId) return;
    const data = await request<{ queues: Queue[] }>(`/v1/projects/${projectId}/queues`);
    setQueues(data.queues);
    setSelectedQueueId((current) => {
      if (data.queues.some((queue) => queue.id === current)) return current;
      return data.queues[0]?.id || "";
    });
  }

  async function loadOperationalData(projectId: string, queueId: string) {
    const metricData = await request<Metrics>("/v1/metrics");
    setMetrics(metricData);

    if (projectId) {
      const dlqData = await request<{ entries: DlqEntry[] }>(`/v1/dlq?project_id=${projectId}`);
      setDlq(dlqData.entries);
    }

    if (queueId) {
      const queueStats = await request<QueueStats>(`/v1/queues/${queueId}/stats`);
      setStats(queueStats);
    }
  }

  useEffect(() => {
    if (!token) return;
    loadProjects().catch((error) => setMessage(error.message));
  }, [token]);

  useEffect(() => {
    if (!token || !selectedProjectId) return;
    loadQueues(selectedProjectId).catch((error) => setMessage(error.message));
  }, [token, selectedProjectId]);

  useEffect(() => {
    if (!token) return;
    loadOperationalData(selectedProjectId, selectedQueueId).catch((error) => setMessage(error.message));
  }, [token, selectedProjectId, selectedQueueId]);

  async function submitAuth(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setMessage("");
    try {
      if (mode === "register") {
        await parseResponse(
          await fetch(`${API_BASE}/v1/auth/register`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email, password, display_name: displayName })
          })
        );
      }

      const data = await parseResponse<{ access_token: string }>(
        await fetch(`${API_BASE}/v1/auth/login`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password })
        })
      );
      localStorage.setItem("scheduler_token", data.access_token);
      setToken(data.access_token);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Authentication failed");
    } finally {
      setBusy(false);
    }
  }

  async function createProject(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const formEl = event.currentTarget;
    const form = new FormData(formEl);
    const name = String(form.get("projectName") || "");
    const organization = String(form.get("organizationName") || "");
    setBusy(true);
    try {
      await request<Project>("/v1/projects", {
        method: "POST",
        body: JSON.stringify({
          organization_name: organization,
          organization_slug: slugify(organization),
          name,
          slug: slugify(name),
          description: String(form.get("projectDescription") || "")
        })
      });
      formEl.reset();
      await loadProjects();
      setMessage("Project created");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Project creation failed");
    } finally {
      setBusy(false);
    }
  }

  async function createQueue(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedProjectId) return;
    const formEl = event.currentTarget;
    const form = new FormData(formEl);
    setBusy(true);
    try {
      await request<Queue>("/v1/queues", {
        method: "POST",
        body: JSON.stringify({
          project_id: selectedProjectId,
          name: String(form.get("queueName") || ""),
          description: String(form.get("queueDescription") || ""),
          concurrency_limit: Number(form.get("concurrency") || 10)
        })
      });
      formEl.reset();
      await loadQueues(selectedProjectId);
      setMessage("Queue created");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Queue creation failed");
    } finally {
      setBusy(false);
    }
  }

  async function createJob(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedQueueId) return;
    const form = new FormData(event.currentTarget);
    setBusy(true);
    try {
      const jobType = String(form.get("jobType") || "immediate");
      const payloadText = String(form.get("payload") || "{}");
      await request(`/v1/queues/${selectedQueueId}/jobs`, {
        method: "POST",
        body: JSON.stringify({
          name: String(form.get("jobName") || ""),
          payload: JSON.parse(payloadText),
          job_type: jobType,
          priority: String(form.get("priority") || "normal"),
          max_attempts: Number(form.get("maxAttempts") || 3),
          scheduled_at: jobType === "scheduled" || jobType === "delayed" ? String(form.get("scheduledAt")) : null,
          cron_expression: jobType === "recurring" ? String(form.get("cronExpression")) : null,
          idempotency_key: String(form.get("idempotencyKey") || "") || null
        })
      });
      await loadOperationalData(selectedProjectId, selectedQueueId);
      setMessage("Job submitted");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Job submission failed");
    } finally {
      setBusy(false);
    }
  }

  async function toggleQueue(paused: boolean) {
    if (!selectedQueueId) return;
    await request(`/v1/queues/${selectedQueueId}/${paused ? "resume" : "pause"}`, { method: "POST" });
    await loadQueues(selectedProjectId);
  }

  async function retryDlq(entryId: string) {
    await request(`/v1/dlq/${entryId}/retry`, { method: "POST" });
    await loadOperationalData(selectedProjectId, selectedQueueId);
  }

  if (!token) {
    return (
      <main className="auth-page">
        <section className="auth-panel">
          <div className="brand-row">
            <span className="brand-mark"><Workflow size={22} /></span>
            <div>
              <h1>Scheduler Control Plane</h1>
              <p>Distributed job operations dashboard</p>
            </div>
          </div>
          <div className="mode-toggle" role="tablist">
            <button className={mode === "login" ? "active" : ""} onClick={() => setMode("login")}>Login</button>
            <button className={mode === "register" ? "active" : ""} onClick={() => setMode("register")}>Register</button>
          </div>
          <form onSubmit={submitAuth} className="stack">
            {mode === "register" && (
              <label>
                Display name
                <input value={displayName} onChange={(event) => setDisplayName(event.target.value)} />
              </label>
            )}
            <label>
              Email
              <input type="email" value={email} onChange={(event) => setEmail(event.target.value)} />
            </label>
            <label>
              Password
              <input type="password" value={password} onChange={(event) => setPassword(event.target.value)} />
            </label>
            <button className="primary" disabled={busy}>
              <ShieldCheck size={17} />
              {mode === "login" ? "Sign in" : "Create account"}
            </button>
          </form>
          {message && <p className="notice error">{message}</p>}
        </section>
      </main>
    );
  }

  const statTiles = [
    ["Queued", stats?.queued_count ?? metrics.queued_jobs ?? 0, Clock3],
    ["Running", stats?.running_count ?? metrics.running_jobs ?? 0, Activity],
    ["Completed", stats?.completed_count ?? metrics.completed_jobs ?? 0, CheckCircle2],
    ["DLQ", stats?.dlq_count ?? metrics.dlq_count ?? 0, AlertTriangle]
  ] as const;

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand-row compact">
          <span className="brand-mark"><Workflow size={20} /></span>
          <strong>Scheduler</strong>
        </div>
        <label>
          Project
          <select value={selectedProjectId} onChange={(event) => setSelectedProjectId(event.target.value)}>
            <option value="">Select project</option>
            {projects.map((project) => (
              <option key={project.id} value={project.id}>{project.name}</option>
            ))}
          </select>
        </label>
        <label>
          Queue
          <select value={selectedQueueId} onChange={(event) => setSelectedQueueId(event.target.value)}>
            <option value="">Select queue</option>
            {queues.map((queue) => (
              <option key={queue.id} value={queue.id}>{queue.name}</option>
            ))}
          </select>
        </label>
        <button className="ghost" onClick={() => loadOperationalData(selectedProjectId, selectedQueueId)}>
          <RefreshCw size={16} />
          Refresh
        </button>
        <button
          className="ghost danger"
          onClick={() => {
            localStorage.removeItem("scheduler_token");
            setToken("");
          }}
        >
          <LogOut size={16} />
          Sign out
        </button>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Control plane</p>
            <h1>{selectedProject?.name ?? "Operational Dashboard"}</h1>
          </div>
          <div className="status-pill">
            <Server size={16} />
            API connected
          </div>
        </header>

        {message && <p className="notice">{message}</p>}

        <section className="metrics-grid">
          {statTiles.map(([label, value, Icon]) => (
            <article className="metric-card" key={label}>
              <Icon size={19} />
              <span>{label}</span>
              <strong>{value}</strong>
            </article>
          ))}
        </section>

        <section className="two-column">
          <form className="panel stack" onSubmit={createProject}>
            <h2>Project</h2>
            <label>Organization<input name="organizationName" placeholder="Acme Operations" required /></label>
            <label>Project name<input name="projectName" placeholder="Primary Scheduler" required /></label>
            <label>Description<input name="projectDescription" placeholder="Production background work" /></label>
            <button className="secondary" disabled={busy}><Plus size={16} />Create project</button>
          </form>

          <form className="panel stack" onSubmit={createQueue}>
            <h2>Queue</h2>
            <label>Queue name<input name="queueName" placeholder="critical-work" required /></label>
            <label>Description<input name="queueDescription" placeholder="Latency-sensitive jobs" /></label>
            <label>Concurrency<input name="concurrency" type="number" min="1" defaultValue="10" /></label>
            <button className="secondary" disabled={busy || !selectedProjectId}><Plus size={16} />Create queue</button>
          </form>
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Submit Job</h2>
              <p>{selectedQueue ? `${selectedQueue.name} · concurrency ${selectedQueue.concurrency_limit}` : "Select or create a queue"}</p>
            </div>
            {selectedQueue && (
              <button className="ghost" onClick={() => toggleQueue(selectedQueue.is_paused)}>
                {selectedQueue.is_paused ? <Play size={16} /> : <Pause size={16} />}
                {selectedQueue.is_paused ? "Resume" : "Pause"}
              </button>
            )}
          </div>
          <form className="job-form" onSubmit={createJob}>
            <label>Job name<input name="jobName" placeholder="send-email" required /></label>
            <label>Type
              <select name="jobType">
                <option value="immediate">Immediate</option>
                <option value="scheduled">Scheduled</option>
                <option value="delayed">Delayed</option>
                <option value="recurring">Recurring</option>
                <option value="batch">Batch</option>
              </select>
            </label>
            <label>Priority
              <select name="priority">
                <option value="normal">Normal</option>
                <option value="high">High</option>
                <option value="critical">Critical</option>
                <option value="low">Low</option>
              </select>
            </label>
            <label>Max attempts<input name="maxAttempts" type="number" min="1" defaultValue="3" /></label>
            <label>Scheduled at<input name="scheduledAt" type="datetime-local" /></label>
            <label>Cron<input name="cronExpression" placeholder="0/30 * * * * * *" /></label>
            <label>Idempotency key<input name="idempotencyKey" placeholder="client-request-123" /></label>
            <label className="payload">Payload<textarea name="payload" defaultValue={'{"customer_id":"cus_123","dry_run":true}'} /></label>
            <button className="primary" disabled={busy || !selectedQueueId}><Send size={16} />Submit job</button>
          </form>
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Dead Letter Queue</h2>
              <p>Failed jobs awaiting operator action</p>
            </div>
            <BarChart3 size={20} />
          </div>
          <div className="table">
            <div className="table-row table-head">
              <span>Job</span><span>Attempts</span><span>Error</span><span>Action</span>
            </div>
            {dlq.length === 0 && <div className="empty">No DLQ entries for this project.</div>}
            {dlq.map((entry) => (
              <div className="table-row" key={entry.id}>
                <span>{entry.job_name}</span>
                <span>{entry.attempt_count}</span>
                <span className="truncate">{entry.last_error ?? "No error recorded"}</span>
                <button className="small" disabled={!!entry.manually_retried_at} onClick={() => retryDlq(entry.id)}>
                  <RefreshCw size={14} />
                  Retry
                </button>
              </div>
            ))}
          </div>
        </section>
      </section>
    </main>
  );
}
