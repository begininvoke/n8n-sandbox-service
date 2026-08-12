import { exec, resumeExecution, deleteExecution } from "./exec";
import {
  readFile,
  writeFile,
  appendFile,
  deleteFile,
  copyFile,
  moveFile,
  mkdir,
  listFiles,
  stat,
} from "./files";
import { HttpClient } from "./http";
import {
  createJob,
  stageJobFile,
  startJob,
  resumeJobEvents,
  getJob,
  getJobFile,
  deleteJob,
} from "./jobs";
import { createSandbox, getSandbox, deleteSandbox } from "./sandboxes";
import type {
  CopyFileRequest,
  CreateSandboxOptions,
  DeleteFileOptions,
  ExecRequest,
  ExecResult,
  FileContent,
  FileEntry,
  FileStat,
  JobRecord,
  JobResult,
  JobSpec,
  ListFilesOptions,
  MoveFileRequest,
  SandboxClientOptions,
  SandboxRecord,
  StartJobOptions,
} from "./types";

/**
 * High-level client for interacting with the sandbox service HTTP API.
 */
export class SandboxClient {
  private readonly http: HttpClient;

  /**
   * Creates a sandbox service client.
   */
  constructor(options: SandboxClientOptions) {
    this.http = new HttpClient(options.baseUrl ?? "", options.apiKey, options.retry);
  }

  // #region Sandbox lifecycle

  /**
   * Creates a new sandbox.
   */
  async createSandbox(options?: CreateSandboxOptions): Promise<SandboxRecord> {
    return createSandbox(this.http, options);
  }

  /**
   * Fetches a sandbox by ID.
   */
  async getSandbox(id: string): Promise<SandboxRecord> {
    return getSandbox(this.http, id);
  }

  /**
   * Deletes a sandbox by ID.
   */
  async deleteSandbox(id: string): Promise<void> {
    return deleteSandbox(this.http, id);
  }

  // #endregion
  // #region Command execution

  /**
   * Executes a command inside a sandbox.
   */
  async exec(id: string, request: ExecRequest): Promise<ExecResult> {
    return exec(this.http, id, request);
  }

  /**
   * Resumes or replays an execution, returning the aggregated result.
   */
  async resumeExecution(sandboxId: string, execId: string, afterSeq?: number): Promise<ExecResult> {
    return resumeExecution(this.http, sandboxId, execId, afterSeq);
  }

  /**
   * Cancels and deletes an execution.
   */
  async deleteExecution(sandboxId: string, execId: string): Promise<void> {
    return deleteExecution(this.http, sandboxId, execId);
  }

  // #endregion
  // #region File operations

  /**
   * Reads a file from a sandbox.
   */
  async readFile(id: string, path: string): Promise<Buffer> {
    return readFile(this.http, id, path);
  }

  /**
   * Writes a file into a sandbox.
   */
  async writeFile(
    id: string,
    path: string,
    content: FileContent,
    overwrite?: boolean,
  ): Promise<void> {
    return writeFile(this.http, id, path, content, overwrite);
  }

  /**
   * Appends content to a file in a sandbox.
   */
  async appendFile(id: string, path: string, content: FileContent): Promise<void> {
    return appendFile(this.http, id, path, content);
  }

  /**
   * Deletes a file or directory from a sandbox.
   */
  async deleteFile(id: string, path: string, options?: DeleteFileOptions): Promise<void> {
    return deleteFile(this.http, id, path, options);
  }

  /**
   * Copies a file or directory inside a sandbox.
   */
  async copyFile(id: string, request: CopyFileRequest): Promise<void> {
    return copyFile(this.http, id, request);
  }

  /**
   * Moves or renames a file or directory inside a sandbox.
   */
  async moveFile(id: string, request: MoveFileRequest): Promise<void> {
    return moveFile(this.http, id, request);
  }

  /**
   * Creates a directory inside a sandbox.
   */
  async mkdir(id: string, path: string, recursive?: boolean): Promise<void> {
    return mkdir(this.http, id, path, recursive);
  }

  /**
   * Lists files in a sandbox directory.
   */
  async listFiles(id: string, options?: ListFilesOptions): Promise<FileEntry[]> {
    return listFiles(this.http, id, options);
  }

  /**
   * Returns metadata for a sandbox file or directory.
   */
  async stat(id: string, path: string): Promise<FileStat> {
    return stat(this.http, id, path);
  }

  // #endregion
  // #region Jobs (one-shot containers)

  /**
   * Creates a job for running a one-shot container from an arbitrary image.
   */
  async createJob(opts: JobSpec): Promise<JobRecord> {
    return createJob(this.http, opts);
  }

  /**
   * Stages a file into a job's input directory before it starts.
   */
  async stageJobFile(id: string, path: string, data: FileContent): Promise<void> {
    return stageJobFile(this.http, id, path, data);
  }

  /**
   * Starts a job and streams its output until the job exits.
   */
  async startJob(id: string, opts?: StartJobOptions): Promise<JobResult> {
    return startJob(this.http, id, opts);
  }

  /**
   * Resumes watching a job's event stream after a disconnect, from the last seen sequence
   * number (omit to replay the full history).
   */
  async resumeJobEvents(id: string, afterSeq?: number, opts?: StartJobOptions): Promise<JobResult> {
    return resumeJobEvents(this.http, id, afterSeq, opts);
  }

  /**
   * Fetches a job by ID.
   */
  async getJob(id: string): Promise<JobRecord> {
    return getJob(this.http, id);
  }

  /**
   * Reads a file from a job's output directory.
   */
  async getJobFile(id: string, path: string): Promise<Buffer> {
    return getJobFile(this.http, id, path);
  }

  /**
   * Deletes a job by ID.
   */
  async deleteJob(id: string): Promise<void> {
    return deleteJob(this.http, id);
  }

  // #endregion
}
