from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional

from ..core.utils import U
from ..ssh.ssh_client import SSHClient
from ..ssh.ssh_config import SSHConfig
from ..vmware.vmdk_parser import VMDK
import os
class Fetch:
    @staticmethod
    def fetch_descriptor_and_extent(
        logger: logging.Logger,
        sshc: SSHClient,
        remote_desc: str,
        outdir: Path,
        fetch_all: bool,
    ) -> Path:
        U.banner(logger, "Fetch VMDK from remote")
        U.ensure_dir(outdir)
        sshc.check()
        if not sshc.exists(remote_desc):
            U.die(logger, f"Remote descriptor not found: {remote_desc}", 1)
        local_desc = outdir / os.path.basename(remote_desc)
        logger.info(f"Copying descriptor: {remote_desc}")
        sshc.scp_from(remote_desc, local_desc)
        extent_rel = VMDK.parse_extent(logger, local_desc)
        remote_dir = os.path.dirname(remote_desc)
        if extent_rel:
            remote_extent = os.path.join(remote_dir, extent_rel)
        else:
            stem = local_desc.stem
            remote_extent = os.path.join(remote_dir, f"{stem}-flat.vmdk")
        if sshc.exists(remote_extent):
            logger.info(f"Copying extent: {remote_extent}")
            sshc.scp_from(remote_extent, outdir / os.path.basename(remote_extent))
        else:
            logger.warning(f"Extent not found remotely: {remote_extent}")
        if fetch_all:
            cur = local_desc
            while True:
                parent = VMDK.parse_parent(logger, cur)
                if not parent:
                    break
                remote_parent = os.path.join(remote_dir, parent)
                local_parent = outdir / parent
                if sshc.exists(remote_parent):
                    logger.info(f"Copying parent descriptor: {remote_parent}")
                    sshc.scp_from(remote_parent, local_parent)
                    cur = local_parent
                else:
                    logger.warning(f"Parent descriptor missing: {remote_parent}")
                    break
        return local_desc
