"""
Remote-launch utilities for the experiment harness (the `remote` config section).

Remote launch assumes bare `ssh <host>` works without a prompt (BatchMode) and that every
machine holds the same code at the same path relative to its $HOME. A "remote" host may be
this machine (`ssh node0` loopback); `remote_preflight` detects that via a token file, and
loopback hosts then need no remote cleanup, prebuild, or copy-back because their roles
write into the local result tree directly.

This module is a library for run.py (not a CLI). It honors run.py's --dry-run flag through
the module-level `dry_run` attribute, which run.py sets alongside its own.
"""

import logging
import shlex
import socket
import subprocess
import uuid
from pathlib import Path

from config import REPO_ROOT, RemoteEntry

logger = logging.getLogger(__name__)
dry_run = False

# Non-interactive ssh for remote replication roles: fail fast instead of prompting.
SSH_OPTS = ["-o", "BatchMode=yes"]


def remote_repo_path() -> str:
    """Repository path on a remote machine, relative to its $HOME. Remote launch assumes
    every machine holds the same code at the same home-relative path."""
    try:
        return str(REPO_ROOT.relative_to(Path.home()))
    except ValueError:
        raise RuntimeError(
            f"remote launch requires the repo under $HOME "
            f"(repo: {REPO_ROOT}, home: {Path.home()})"
        )


def ssh_command(host: str, shell_cmd: str) -> list[str]:
    return ["ssh", *SSH_OPTS, host, shell_cmd]


def run_ssh(host: str, shell_cmd: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        ssh_command(host, shell_cmd), capture_output=True, text=True, timeout=timeout
    )


def resolve_role_ip(role: str, remote: dict[str, RemoteEntry]) -> str:
    """Data-plane IP peers use to reach a replication role in remote mode: the entry's
    explicit ip, else its ssh name resolved locally, else (entry-less local role) the local
    short hostname resolved locally. Must be numeric: the bench parses host params with
    IPAddress.Parse."""
    entry = remote.get(role)
    if entry is not None and entry.ip is not None:
        return entry.ip
    name = (
        entry.ssh
        if entry is not None and entry.ssh is not None
        else socket.gethostname().split(".")[0]
    )
    try:
        ip = socket.gethostbyname(name)
    except OSError as e:
        raise RuntimeError(
            f"cannot resolve an IP for the {role} role from '{name}'; "
            f"set remote.{role}.ip explicitly"
        ) from e
    if ip.startswith("127."):
        logger.warning(
            f"{role} resolved to loopback address {ip} (from '{name}'); "
            f"unreachable from other machines -- set remote.{role}.ip if any peer is remote"
        )
    return ip


def remote_role_output(exp_name: str, run_name: str, role: str) -> str:
    """Role output path relative to the remote repo root; mirrors the local layout so a
    loopback 'remote' writes directly into the local result tree."""
    return f"result/{exp_name}/{run_name}/{role}/output.txt"


def ssh_role_command(host: str, cmd: list[str], output_rel: str) -> list[str]:
    """Full ssh command launching a replication role on a remote machine. `cmd` is the
    role's command as it should execute on the remote machine (project path relative to
    the repo root, numactl affinity prefix included); its stdout/stderr are redirected on
    the remote side into the role's result subdir (copied back after the run, or already
    local when the host loopbacks)."""
    shell_cmd = (
        f"cd {shlex.quote(remote_repo_path())} && "
        f"mkdir -p {shlex.quote(str(Path(output_rel).parent))} && "
        f"exec {shlex.join(cmd)} > {shlex.quote(output_rel)} 2>&1"
    )
    return ssh_command(host, shell_cmd)


def launch_ssh_role(full_cmd: list[str], ssh_log_path: Path, role: str, host: str):
    """Start a remote role; the local file captures ssh-level diagnostics only (the role's
    own output is redirected on the remote side)."""
    logger.info(f"Launch {role} on {host}: {shlex.join(full_cmd)}")
    if dry_run:
        logger.info("[dry-run] skipping launch")
        return None
    ssh_log_path.parent.mkdir(parents=True, exist_ok=True)
    log_f = open(ssh_log_path, "w")
    # Own session so the local launcher can be torn down as a group by run.py.
    return subprocess.Popen(
        full_cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=str(ssh_log_path.parent),
        start_new_session=True,
    )


def kill_remote_role(host: str | None, pattern: str) -> None:
    """Terminate a remote role's whole process tree. Closing the ssh channel does not kill
    the remote command, and `dotnet run` spawns an app-host child that outlives a kill of
    the wrapper alone; the pattern matches both (the project path and the app-host binary
    both contain the server name plus the port), so pkill signals the entire tree. SIGTERM
    first for a clean port release, then SIGKILL for anything still alive."""
    if dry_run or host is None:
        return
    q = shlex.quote(pattern)
    run_ssh(
        host,
        f"pkill -TERM -f -- {q} 2>/dev/null; sleep 1; pkill -KILL -f -- {q} 2>/dev/null; true",
    )


def rsync_back(host: str, remote_rel_dir: str, local_dir: Path) -> None:
    """Pull a remote role's result subdir into the local result tree. remote_rel_dir is
    relative to the remote $HOME."""
    if dry_run:
        return
    local_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "rsync",
        "-a",
        "-e",
        shlex.join(["ssh", *SSH_OPTS]),
        f"{host}:{remote_rel_dir}/",
        f"{local_dir}/",
    ]
    logger.debug(f"[copy-back] {shlex.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        logger.error(
            f"copy-back from {host}:{remote_rel_dir} failed: {res.stderr.strip()}"
        )


def remote_preflight(spec, exp_dir: Path) -> dict[str, bool]:
    """Per unique ssh host: reachability + repo presence checks, leftover process cleanup,
    stale-result wipe, and a prebuild of the projects its roles run (so the ready-timeout
    never races a cold build). Returns host -> loopback, where loopback means `ssh host`
    lands on this machine: a freshly written token is visible at the same home-relative
    path, so its roles write into the local result tree directly and need no cleanup or
    copy-back."""
    rel = remote_repo_path()
    hosts = sorted({e.ssh for e in spec.remote.values() if e.ssh is not None})
    # The primary/replica slots run the server project; the client slot runs the bench.
    host_projects: dict[str, set[str]] = {host: set() for host in hosts}
    for role, entry in spec.remote.items():
        if entry.ssh is not None:
            host_projects[entry.ssh].add(
                spec.benchmark_project if role == "client" else spec.server_project
            )
    token = uuid.uuid4().hex
    if dry_run:
        for host in hosts:
            logger.info(f"[dry-run] remote preflight on {host}")
        return dict.fromkeys(hosts, False)

    # Trailing newline so the probe's `cat` output stays its own line.
    (exp_dir / ".token").write_text(token + "\n")
    loopback: dict[str, bool] = {}
    for host in hosts:
        # `; true` keeps the exit code about ssh itself; repo presence is signaled by the
        # repo-ok line instead.
        probe = run_ssh(
            host,
            f"cat {shlex.quote(f'{rel}/result/{spec.name}/.token')} 2>/dev/null; "
            f"test -d {shlex.quote(rel)} && echo repo-ok; true",
        )
        if probe.returncode != 0:
            raise RuntimeError(
                f"ssh {host} failed (is it reachable with BatchMode?): "
                f"{probe.stderr.strip()}"
            )
        out_lines = probe.stdout.split()
        if "repo-ok" not in out_lines:
            raise RuntimeError(
                f"ssh {host}: repo not found at ~/{rel} "
                f"(remote launch assumes the same home-relative path)"
            )
        loopback[host] = token in out_lines
        if loopback[host]:
            logger.info(f"Remote host {host} is this machine (loopback)")
            continue
        if spec.affinity.any_set():
            numactl = run_ssh(host, "which numactl")
            if numactl.returncode != 0:
                raise RuntimeError(f"ssh {host}: affinity configured but 'numactl' not found")
        # Kill leftover processes and wipe stale results in SEPARATE ssh calls. `pkill -f
        # <stem>` matches the ssh wrapper's own command line (the stem text appears in it),
        # so that wrapper is among the killed processes; combining it with the wipe in one
        # command would kill the wrapper before `rm -rf` ran, leaking stale cluster state
        # (a recovered nodes.conf re-grants slots and breaks the next bootstrap). The wipe's
        # own command line contains no stem, so it always runs.
        run_ssh(
            host,
            f"pkill -f {shlex.quote(Path(spec.benchmark_project).stem)} 2>/dev/null; "
            f"pkill -f {shlex.quote(Path(spec.server_project).stem)} 2>/dev/null; true",
        )
        run_ssh(host, f"rm -rf {shlex.quote(f'{rel}/result/{spec.name}')}")
        for project in sorted(host_projects[host]):
            logger.info(f"Prebuilding {Path(project).stem} on {host} ...")
            build = run_ssh(
                host,
                f"cd {shlex.quote(rel)} && dotnet build -c Release {shlex.quote(project)}",
                timeout=600,
            )
            if build.returncode != 0:
                raise RuntimeError(
                    f"ssh {host}: dotnet build failed:\n{build.stdout[-2000:]}{build.stderr[-2000:]}"
                )
    return loopback
