"""SSH connection via system ssh with ControlMaster. No paramiko dependency.

One master connection is established on connect(). All subsequent operations
(exec, rsync, tar, sftp) reuse it through the control socket. Passphrase is
prompted once by the system ssh binary.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from io import BytesIO
from pathlib import Path

from delta.exceptions import ConnectionError, RemoteCommandError
from delta.models import FileInfo, SSHConfig, TransferConfig, TransferMethod

logger = logging.getLogger("delta")


class Connection:
    """Manages an SSH ControlMaster connection to a device."""

    def __init__(self, ssh_config: SSHConfig, transfer_config: TransferConfig | None = None):
        self._ssh = ssh_config
        self._transfer = transfer_config or TransferConfig()
        self._socket_path: str = ""
        self._connected: bool = False
        self._resolved_method: TransferMethod | None = None

    def __enter__(self) -> Connection:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Establish SSH ControlMaster connection."""
        if self._connected:
            return

        from delta import ui
        ui.print_phase("CONNECTING")
        ui.print_info(f"{self._ssh.user}@{self._ssh.host}:{self._ssh.port}")

        # Create socket in a temp directory (must be short path — Unix limit ~108 chars)
        sock_dir = tempfile.mkdtemp(prefix="delta_ssh_")
        self._socket_path = os.path.join(sock_dir, "ctrl.sock")

        # Start ControlMaster in background (-f = background after auth, -N = no command, -M = master)
        cmd = self._ssh_base_cmd() + [
            "-fNM",
            "-o", "ControlPersist=yes",
            "-o", f"ControlPath={self._socket_path}",
            f"{self._ssh.user}@{self._ssh.host}",
        ]

        try:
            result = subprocess.run(
                cmd, timeout=self._ssh.connect_timeout, capture_output=True, text=True,
            )
            if result.returncode != 0:
                stderr = result.stderr.strip()
                raise ConnectionError(
                    f"Failed to connect to {self._ssh.host}: {stderr}"
                )
        except subprocess.TimeoutExpired:
            raise ConnectionError(
                f"Connection to {self._ssh.host} timed out after {self._ssh.connect_timeout}s"
            )
        except FileNotFoundError:
            raise ConnectionError(
                "ssh binary not found. Ensure OpenSSH client is installed."
            )

        self._connected = True
        ui.print_success(f"Connected to {self._ssh.host}.")

    def close(self) -> None:
        """Close the ControlMaster connection."""
        if not self._connected:
            return

        # Send exit command to master
        cmd = [
            "ssh",
            "-o", f"ControlPath={self._socket_path}",
            "-O", "exit",
            f"{self._ssh.user}@{self._ssh.host}",
        ]
        subprocess.run(cmd, capture_output=True, timeout=10)

        # Clean up socket directory
        sock_dir = os.path.dirname(self._socket_path)
        if os.path.exists(sock_dir):
            shutil.rmtree(sock_dir, ignore_errors=True)

        self._connected = False
        logger.info("SSH connection closed.")

    def _ssh_base_cmd(self) -> list[str]:
        """Build base ssh command with port and key options."""
        cmd = [
            "ssh",
            "-p", str(self._ssh.port),
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=no",
        ]
        if self._ssh.key_file:
            cmd += ["-i", os.path.expanduser(self._ssh.key_file)]
        return cmd

    def _ssh_cmd(self) -> list[str]:
        """Build ssh command that uses the ControlMaster socket."""
        return [
            "ssh",
            "-o", f"ControlPath={self._socket_path}",
            "-p", str(self._ssh.port),
            f"{self._ssh.user}@{self._ssh.host}",
        ]

    def _ssh_cmd_str(self) -> str:
        """SSH command as a string (for rsync -e)."""
        return f"ssh -o ControlPath={self._socket_path} -p {self._ssh.port}"

    # ------------------------------------------------------------------
    # Transfer method resolution
    # ------------------------------------------------------------------

    def _resolve_method(self) -> TransferMethod:
        """Resolve 'auto' to a concrete method by probing the device."""
        if self._resolved_method is not None:
            return self._resolved_method

        method = self._transfer.method
        if method != TransferMethod.AUTO:
            if method == TransferMethod.RSYNC and not self._has_rsync():
                raise ConnectionError(
                    "Transfer method 'rsync' selected but rsync is not available "
                    "on the device. Change transfer.method to 'tar' or 'sftp' "
                    "in .delta/config.yaml"
                )
            self._resolved_method = method
            logger.info("Transfer method: %s", method.value)
            return method

        # Auto: try rsync → tar → sftp
        if self._has_rsync():
            logger.info("Transfer method: rsync (auto-detected)")
            self._resolved_method = TransferMethod.RSYNC
        elif self._has_tar():
            logger.info("Transfer method: tar (auto-detected)")
            self._resolved_method = TransferMethod.TAR
        else:
            logger.info("Transfer method: sftp (fallback)")
            self._resolved_method = TransferMethod.SFTP

        return self._resolved_method

    def _has_rsync(self) -> bool:
        _, _, code = self.exec("which rsync", check=False, timeout=10)
        return code == 0

    def _has_tar(self) -> bool:
        _, _, code = self.exec("which tar", check=False, timeout=10)
        return code == 0

    # ------------------------------------------------------------------
    # Remote command execution
    # ------------------------------------------------------------------

    def exec(
        self, cmd: str, *, check: bool = True, timeout: int = 300,
    ) -> tuple[str, str, int]:
        """Execute a command on the device. Returns (stdout, stderr, exit_code)."""
        ssh_cmd = self._ssh_cmd() + [cmd]

        try:
            result = subprocess.run(
                ssh_cmd, capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            if check:
                raise RemoteCommandError(f"Command timed out ({timeout}s): {cmd}")
            return "", f"Timed out after {timeout}s", 1

        if check and result.returncode != 0:
            raise RemoteCommandError(
                f"Command failed (exit {result.returncode}): {cmd}\n"
                f"stderr: {result.stderr.strip()}"
            )
        return result.stdout, result.stderr, result.returncode

    def exec_stream(
        self, cmd: str, *, line_callback=None, stderr_callback=None,
        timeout: int = 600,
    ) -> tuple[str, str, int]:
        """Execute a command with real-time stdout/stderr streaming.

        line_callback(line) called for each stdout line.
        stderr_callback(line) called for each stderr line.
        Returns (full_stdout, full_stderr, exit_code).
        """
        import selectors

        ssh_cmd = self._ssh_cmd() + [cmd]

        proc = subprocess.Popen(
            ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )

        sel = selectors.DefaultSelector()
        sel.register(proc.stdout, selectors.EVENT_READ)
        sel.register(proc.stderr, selectors.EVENT_READ)

        stdout_lines: list[str] = []
        stderr_lines: list[str] = []

        try:
            while proc.poll() is None or sel.get_map():
                events = sel.select(timeout=0.5)
                for key, _ in events:
                    line = key.fileobj.readline()
                    if not line:
                        sel.unregister(key.fileobj)
                        continue
                    if key.fileobj is proc.stdout:
                        stdout_lines.append(line)
                        if line_callback:
                            line_callback(line.rstrip("\n"))
                    else:
                        stderr_lines.append(line)
                        if stderr_callback:
                            stderr_callback(line.rstrip("\n"))
        except KeyboardInterrupt:
            proc.terminate()
            raise
        finally:
            sel.close()

        proc.wait()
        return "".join(stdout_lines), "".join(stderr_lines), proc.returncode

    def exec_binary(self, cmd: str, *, timeout: int = 600) -> tuple[bytes, int]:
        """Execute a command and return raw stdout bytes (for tar streams)."""
        ssh_cmd = self._ssh_cmd() + [cmd]
        result = subprocess.run(ssh_cmd, capture_output=True, timeout=timeout)
        return result.stdout, result.returncode

    # ------------------------------------------------------------------
    # File listing (always SSH, independent of transfer backend)
    # ------------------------------------------------------------------

    def list_files(
        self, paths: list[str], ignore_patterns: list[str] | None = None,
    ) -> list[FileInfo]:
        """List all files with md5, owner, group, mode.

        Uses two fast passes instead of per-file process spawning:
          1. find -printf for metadata (zero subprocesses)
          2. find -exec md5sum {} + for checksums (batched)
        Then merges results by path.
        """
        if not paths:
            return []

        find_paths = " ".join(f"'{p}'" for p in paths)

        ignore_clause = ""
        if ignore_patterns:
            pattern_args = "|".join(ignore_patterns)
            ignore_clause = f" | grep -vE '{pattern_args}'"

        # Pass 1: metadata via find -printf (no subprocess per file)
        # %p=path, %U=owner, %G=group, %m=mode, %s=size, %l=symlink target
        meta_script = (
            f"find {find_paths} "
            f"\\( -type f -printf 'FILE|||%p|||%U|||%G|||%m|||%s\\n' \\) "
            f"-o \\( -type l -printf 'SYMLINK|||%p|||%l\\n' \\) "
            f"2>/dev/null"
            f"{ignore_clause}"
        )
        meta_out, _, _ = self.exec(meta_script, check=False, timeout=600)

        # Parse metadata
        files_by_path: dict[str, FileInfo] = {}
        symlinks: list[FileInfo] = []

        for line in meta_out.strip().splitlines():
            if not line:
                continue
            parts = line.split("|||")
            if parts[0] == "FILE" and len(parts) >= 6:
                fpath = parts[1]
                files_by_path[fpath] = FileInfo(
                    path=fpath,
                    owner=parts[2], group=parts[3], mode=parts[4],
                    size=int(parts[5]) if parts[5].isdigit() else 0,
                )
            elif parts[0] == "SYMLINK" and len(parts) >= 3:
                symlinks.append(FileInfo(
                    path=parts[1], is_symlink=True, symlink_target=parts[2],
                ))

        # Pass 2: batch md5sum (find -exec {} + groups into few invocations)
        if files_by_path:
            md5_script = (
                f"find {find_paths} -type f -exec md5sum {{}} + 2>/dev/null"
                f"{ignore_clause}"
            )
            md5_out, _, _ = self.exec(md5_script, check=False, timeout=600)

            for line in md5_out.strip().splitlines():
                if not line:
                    continue
                # md5sum output: "d41d8cd98f00b204e9800998ecf8427e  /path/to/file"
                # Two spaces between hash and path
                parts = line.split("  ", 1)
                if len(parts) == 2:
                    md5_hash = parts[0].strip()
                    fpath = parts[1].strip()
                    if fpath in files_by_path:
                        files_by_path[fpath].md5 = md5_hash

        return list(files_by_path.values()) + symlinks

    # ------------------------------------------------------------------
    # Unified download / upload (dispatches to backend)
    # ------------------------------------------------------------------

    def download_files(
        self, file_paths: list[str], local_base: Path, *,
        label: str = "Downloading", compress: bool | None = None,
        total_size: int = 0, file_sizes: dict[str, int] | None = None,
    ) -> int:
        if not file_paths:
            return 0
        method = self._resolve_method()
        if method == TransferMethod.RSYNC:
            return self._download_rsync(
                file_paths, local_base, label=label,
                compress=compress, total_size=total_size,
                file_sizes=file_sizes,
            )
        elif method == TransferMethod.TAR:
            return self._download_tar(file_paths, local_base, label=label)
        else:
            return self._download_sftp(file_paths, local_base, label=label)

    def upload_files(
        self, files: list[tuple[Path, str]], *, label: str = "Uploading",
        compress: bool | None = None,
    ) -> int:
        if not files:
            return 0
        method = self._resolve_method()
        if method == TransferMethod.RSYNC:
            return self._upload_rsync(files, label=label, compress=compress)
        elif method == TransferMethod.TAR:
            return self._upload_tar(files, label=label)
        else:
            return self._upload_sftp(files, label=label)

    # ------------------------------------------------------------------
    # Backend: SFTP (per-file via scp, zero extra space)
    # ------------------------------------------------------------------

    def _download_sftp(
        self, file_paths: list[str], local_base: Path, *, label: str,
    ) -> int:
        total = len(file_paths)
        logger.info("%s: %d files via sftp...", label, total)
        local_base.mkdir(parents=True, exist_ok=True)

        for i, remote_path in enumerate(file_paths, 1):
            rel = remote_path.lstrip("/")
            local_path = local_base / rel
            local_path.parent.mkdir(parents=True, exist_ok=True)
            # Use scp via ControlMaster
            scp_cmd = [
                "scp", "-o", f"ControlPath={self._socket_path}",
                "-P", str(self._ssh.port), "-q",
                f"{self._ssh.user}@{self._ssh.host}:{remote_path}",
                str(local_path),
            ]
            subprocess.run(scp_cmd, capture_output=True, timeout=120)
            if (i % 200 == 0) or i == total:
                logger.info("  %s: %d/%d", label, i, total)
        return total

    def _upload_sftp(
        self, files: list[tuple[Path, str]], *, label: str,
    ) -> int:
        total = len(files)
        logger.info("%s: %d files via sftp...", label, total)

        for i, (local_path, remote_path) in enumerate(files, 1):
            # Ensure remote directory exists
            remote_dir = os.path.dirname(remote_path)
            self.exec(f"mkdir -p '{remote_dir}'", check=False)
            scp_cmd = [
                "scp", "-o", f"ControlPath={self._socket_path}",
                "-P", str(self._ssh.port), "-q",
                str(local_path),
                f"{self._ssh.user}@{self._ssh.host}:{remote_path}",
            ]
            subprocess.run(scp_cmd, capture_output=True, timeout=120)
            if (i % 200 == 0) or i == total:
                logger.info("  %s: %d/%d", label, i, total)
        return total

    # ------------------------------------------------------------------
    # Backend: TAR (streaming download, temp file upload)
    # ------------------------------------------------------------------

    def _download_tar(
        self, file_paths: list[str], local_base: Path, *, label: str,
    ) -> int:
        """Download via tar stream. Pipes file list to stdin — no temp file on device."""
        total = len(file_paths)
        local_base.mkdir(parents=True, exist_ok=True)
        logger.info("%s: %d files via tar stream...", label, total)

        file_list = "\n".join(file_paths) + "\n"

        # tar cf - -T - reads file list from stdin, outputs to stdout
        ssh_cmd = self._ssh_cmd() + ["tar cf - -T - 2>/dev/null"]
        try:
            result = subprocess.run(
                ssh_cmd, input=file_list.encode(), capture_output=True, timeout=1800,
            )
        except subprocess.TimeoutExpired:
            logger.warning("Tar download timed out. Falling back to sftp.")
            return self._download_sftp(file_paths, local_base, label=label)

        if not result.stdout:
            logger.warning("Tar stream empty. Falling back to sftp.")
            return self._download_sftp(file_paths, local_base, label=label)

        # Extract tar locally
        buf = BytesIO(result.stdout)
        with tarfile.open(fileobj=buf, mode="r:") as tar:
            for member in tar.getmembers():
                clean_name = member.name.lstrip("/")
                if ".." in clean_name:
                    continue
                member.name = clean_name
                if member.isfile() or member.issym():
                    tar.extract(member, path=str(local_base), filter="data")

        logger.info(
            "%s: %d files extracted (%s).", label, total, _format_bytes(len(result.stdout)),
        )
        return total

    def _upload_tar(
        self, files: list[tuple[Path, str]], *, label: str,
    ) -> int:
        """Upload via tar pipe. Streams directly — no temp tar on device."""
        total = len(files)
        logger.info("%s: %d files via tar pipe...", label, total)

        # Build tar in memory
        buf = BytesIO()
        with tarfile.open(fileobj=buf, mode="w:") as tar:
            for local_path, remote_path in files:
                if not local_path.exists():
                    logger.warning("Skipping missing: %s", local_path)
                    continue
                tar.add(str(local_path), arcname=remote_path.lstrip("/"))

        tar_data = buf.getvalue()

        # Pipe tar to device: ssh ... "tar xf - -C /"
        ssh_cmd = self._ssh_cmd() + ["tar xf - -C /"]
        result = subprocess.run(
            ssh_cmd, input=tar_data, capture_output=True, timeout=600,
        )

        if result.returncode != 0:
            logger.warning(
                "Tar upload failed (exit %d). Falling back to sftp.",
                result.returncode,
            )
            return self._upload_sftp(files, label=label)

        logger.info(
            "%s: %d files deployed (%s).", label, total, _format_bytes(len(tar_data)),
        )
        return total

    # ------------------------------------------------------------------
    # Backend: RSYNC (fast, uses ControlMaster socket)
    # ------------------------------------------------------------------

    def _rsync_base_cmd(self, *, compress: bool | None = None, upload: bool = False) -> list[str]:
        """Build rsync command that reuses the ControlMaster socket."""
        if upload:
            # For upload: don't set owner/group (handled by set_ownership_bulk)
            # This prevents rsync from changing parent directory ownership
            flags = "-rlt"
        else:
            flags = "-av"
        cmd = [
            "rsync", flags,
            "--info=progress2",
            "-e", self._ssh_cmd_str(),
        ]
        use_compress = compress if compress is not None else self._transfer.compress
        if use_compress:
            cmd.append("--compress")
        return cmd

    def _run_rsync(self, cmd: list[str], label: str = "",
                   total_files: int = 0, total_size: int = 0,
                   file_sizes: dict[str, int] | None = None) -> int:
        """Run rsync with progress bar driven by rsync's own byte counters.

        When compress is off: rsync_bytes is accurate, shown as downloaded/total.
        When compress is on: rsync_bytes != file bytes, so we sum file sizes
        as files complete (from file_sizes dict) for the downloaded counter.
        """
        import selectors
        import time
        from delta import ui

        logger.debug("rsync cmd: %s", " ".join(cmd))
        start = time.monotonic()
        use_compress = "--compress" in cmd
        file_sizes = file_sizes or {}

        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            bufsize=0,
        )

        file_count = 0
        completed_file_bytes = 0
        rsync_bytes = 0
        rsync_speed = ""

        log_file = None
        for handler in logging.getLogger("delta").handlers:
            if hasattr(handler, "log_file") and handler.log_file:
                log_file = handler.log_file
                break

        def _draw_progress() -> None:
            parts: list[str] = []

            # File count bar
            if total_files > 0:
                pct = min(file_count / total_files * 100, 100)
                filled = int(pct / 2.5)
                bar = "█" * filled + "░" * (40 - filled)
                parts.append(f"[{bar}] {file_count}/{total_files}")
            else:
                parts.append(f"{file_count} files")

            # Size: always show downloaded / total
            downloaded = rsync_bytes if not use_compress else completed_file_bytes
            if downloaded or total_size:
                if total_size:
                    parts.append(
                        f"{_format_bytes(downloaded)}/{_format_bytes(total_size)}"
                    )
                elif downloaded:
                    parts.append(_format_bytes(downloaded))

            # Speed
            if rsync_speed:
                parts.append(rsync_speed)

            # Always use our own elapsed timer (rsync time is ETA, jumps around)
            elapsed = time.monotonic() - start
            parts.append(ui.format_duration(elapsed))

            sys.stderr.write(f"\r\033[K    {' | '.join(parts)}")
            sys.stderr.flush()

        def _parse_line(stripped: str) -> None:
            nonlocal file_count, completed_file_bytes
            nonlocal rsync_bytes, rsync_speed

            if not stripped:
                return

            # progress2 lines start with whitespace
            if stripped[0].isspace():
                tokens = stripped.split()
                if len(tokens) >= 4:
                    raw_bytes = tokens[0].replace(",", "")
                    if raw_bytes.isdigit():
                        rsync_bytes = int(raw_bytes)
                    if "/s" in tokens[2]:
                        rsync_speed = tokens[2]
                return

            if stripped.startswith("sent ") or stripped.startswith("total size"):
                return
            if stripped.endswith("/"):
                return

            # It's a filename
            file_count += 1
            # Track completed file bytes for compress mode
            # Match against file_sizes (try with and without leading /)
            fsize = file_sizes.get(stripped, 0)
            if not fsize:
                fsize = file_sizes.get("/" + stripped, 0)
            completed_file_bytes += fsize

            sys.stderr.write(f"\r\033[K    {stripped}\n")
            sys.stderr.flush()
            if log_file:
                log_file.write(f"    {stripped}\n")
                log_file.flush()

        sel = selectors.DefaultSelector()
        sel.register(proc.stdout, selectors.EVENT_READ)
        buf = b""

        sys.stderr.write("\033[?25l")  # Hide cursor
        sys.stderr.flush()

        try:
            while proc.poll() is None:
                events = sel.select(timeout=0.5)

                if events:
                    chunk = proc.stdout.read(4096)  # type: ignore[union-attr]
                    if chunk:
                        buf += chunk
                        # rsync progress2 uses \r for in-place updates,
                        # split on both \n and \r
                        while b"\n" in buf or b"\r" in buf:
                            # Find earliest delimiter
                            pos_n = buf.find(b"\n")
                            pos_r = buf.find(b"\r")
                            if pos_n == -1:
                                pos = pos_r
                            elif pos_r == -1:
                                pos = pos_n
                            else:
                                pos = min(pos_n, pos_r)
                            line_bytes = buf[:pos]
                            buf = buf[pos + 1:]
                            _parse_line(
                                line_bytes.decode("utf-8", errors="replace").rstrip()
                            )

                # Always redraw (timer ticks, speed updates)
                _draw_progress()

            # Drain remaining
            remaining = proc.stdout.read()  # type: ignore[union-attr]
            if remaining:
                buf += remaining
            for part in buf.replace(b"\r", b"\n").split(b"\n"):
                _parse_line(part.decode("utf-8", errors="replace").rstrip())

        finally:
            sel.close()
            sys.stderr.write(f"\r\033[K\033[?25h")  # Clear + show cursor
            sys.stderr.flush()

        exit_code = proc.wait()
        elapsed = time.monotonic() - start

        if exit_code == 0:
            final_bytes = rsync_bytes if not use_compress else completed_file_bytes
            size_info = f", {_format_bytes(final_bytes)}" if final_bytes else ""
            logger.info(
                "    %s: completed in %s (%d files%s)",
                label or "rsync", ui.format_duration(elapsed),
                total_files or file_count, size_info,
            )

        return exit_code

    def _download_rsync(
        self, file_paths: list[str], local_base: Path, *,
        label: str, total_size: int = 0, compress: bool | None = None,
        file_sizes: dict[str, int] | None = None,
    ) -> int:
        total = len(file_paths)
        local_base.mkdir(parents=True, exist_ok=True)
        logger.info("%s: %d files via rsync...", label, total)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False,
        ) as f:
            f.write("\n".join(file_paths) + "\n")
            filelist_path = f.name

        try:
            remote = f"{self._ssh.user}@{self._ssh.host}:/"
            cmd = self._rsync_base_cmd(compress=compress) + [
                "--files-from", filelist_path,
                remote,
                str(local_base) + "/",
            ]
            exit_code = self._run_rsync(
                cmd, label=label, total_files=total,
                total_size=total_size, file_sizes=file_sizes,
            )

            if exit_code != 0:
                logger.warning("rsync download failed (exit %d).", exit_code)
                logger.info("Falling back to sftp...")
                return self._download_sftp(file_paths, local_base, label=label)
        finally:
            os.unlink(filelist_path)

        return total

    def _upload_rsync(
        self, files: list[tuple[Path, str]], *, label: str,
        compress: bool | None = None,
    ) -> int:
        total = len(files)
        logger.info("%s: %d files via rsync...", label, total)

        with tempfile.TemporaryDirectory() as staging:
            staging_path = Path(staging)
            file_list: list[str] = []
            upload_sizes: dict[str, int] = {}
            upload_total = 0

            for local_path, remote_path in files:
                if not local_path.exists():
                    logger.warning("Skipping missing: %s", local_path)
                    continue
                rel = remote_path.lstrip("/")
                dest = staging_path / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                try:
                    os.link(str(local_path), str(dest))
                except OSError:
                    shutil.copy2(str(local_path), str(dest))
                file_list.append(rel)
                fsize = local_path.stat().st_size
                upload_sizes[rel] = fsize
                upload_total += fsize

            filelist_file = staging_path / "_delta_filelist.txt"
            filelist_file.write_text("\n".join(file_list) + "\n")

            remote = f"{self._ssh.user}@{self._ssh.host}:/"
            cmd = self._rsync_base_cmd(compress=compress, upload=True) + [
                "--files-from", str(filelist_file),
                "--no-implied-dirs",
                str(staging_path) + "/",
                remote,
            ]
            exit_code = self._run_rsync(
                cmd, label=label, total_files=total,
                total_size=upload_total, file_sizes=upload_sizes,
            )

            if exit_code != 0:
                logger.warning("rsync upload failed (exit %d).", exit_code)
                logger.info("Falling back to sftp...")
                return self._upload_sftp(files, label=label)

        return total

    # ------------------------------------------------------------------
    # Single file operations
    # ------------------------------------------------------------------

    def download_file(self, remote_path: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        scp_cmd = [
            "scp", "-o", f"ControlPath={self._socket_path}",
            "-P", str(self._ssh.port), "-q",
            f"{self._ssh.user}@{self._ssh.host}:{remote_path}",
            str(local_path),
        ]
        subprocess.run(scp_cmd, capture_output=True, timeout=120)

    def upload_file(self, local_path: Path, remote_path: str) -> None:
        remote_dir = os.path.dirname(remote_path)
        self.exec(f"mkdir -p '{remote_dir}'", check=False)
        scp_cmd = [
            "scp", "-o", f"ControlPath={self._socket_path}",
            "-P", str(self._ssh.port), "-q",
            str(local_path),
            f"{self._ssh.user}@{self._ssh.host}:{remote_path}",
        ]
        subprocess.run(scp_cmd, capture_output=True, timeout=120)

    # ------------------------------------------------------------------
    # Ownership, symlinks, deletion
    # ------------------------------------------------------------------

    def set_ownership_bulk(
        self, entries: list[tuple[str, str, str, str]],
    ) -> None:
        """Set ownership for multiple files. Each entry: (path, owner, group, mode)."""
        if not entries:
            return
        lines = [
            f"chown {o}:{g} '{p}' 2>/dev/null; chmod {m} '{p}' 2>/dev/null"
            for p, o, g, m in entries
        ]
        batch_size = 200
        for i in range(0, len(lines), batch_size):
            self.exec(" && ".join(lines[i:i + batch_size]), check=False)
        logger.info("Set ownership for %d files.", len(entries))

    def set_ownership(
        self, remote_path: str, owner: str, group: str, mode: str,
    ) -> None:
        self.exec(
            f"chown {owner}:{group} '{remote_path}' && chmod {mode} '{remote_path}'",
            check=False,
        )

    def create_symlinks(self, symlinks: list[tuple[str, str]]) -> None:
        """Create symlinks on device. Each entry: (link_path, target)."""
        if not symlinks:
            return
        lines = [
            f"mkdir -p '{os.path.dirname(lp)}' && ln -sf '{t}' '{lp}'"
            for lp, t in symlinks
        ]
        batch_size = 200
        for i in range(0, len(lines), batch_size):
            self.exec(" && ".join(lines[i:i + batch_size]), check=False)
        logger.info("Created %d symlinks.", len(symlinks))

    def delete_remote_files(self, paths: list[str]) -> None:
        if not paths:
            return
        batch_size = 200
        for i in range(0, len(paths), batch_size):
            quoted = " ".join(f"'{p}'" for p in paths[i:i + batch_size])
            self.exec(f"rm -f {quoted}", check=False)
        logger.info("Deleted %d files.", len(paths))

    def delete_remote_file(self, remote_path: str) -> None:
        self.exec(f"rm -f '{remote_path}'", check=False)


# ======================================================================
# Utilities
# ======================================================================

def compute_local_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _format_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024  # type: ignore[assignment]
    return f"{n:.1f} TB"
