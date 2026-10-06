#!/usr/bin/env bash
# Host upkeep, as root. Safe to run any number of times: first boot (user data) and every
# deploy run it, so a change here reaches the running host on the next deploy.
#
# - Security updates install daily (dnf-automatic). AL2023 pins packages to the release
#   the AMI shipped with, so it's unpinned to `latest`: a single host has no fleet to
#   stage releases on, and the app runs in containers, so the OS is all that changes.
# - A kernel or core library update needs a reboot. Sundays at 04:00 UTC the host
#   reboots if one is waiting. The stack comes back by itself (livedemos.service) and
#   CloudFront serves the fallback snapshot meanwhile.
set -euo pipefail

echo latest >/etc/dnf/vars/releasever

# dnf-plugins-core gives `dnf needs-restarting`.
for pkg in dnf-automatic dnf-plugins-core; do
  rpm -q "$pkg" >/dev/null || dnf install -y "$pkg"
done
sed -i \
  -e 's/^upgrade_type *=.*/upgrade_type = security/' \
  -e 's/^apply_updates *=.*/apply_updates = yes/' \
  /etc/dnf/automatic.conf
grep -q '^apply_updates = yes' /etc/dnf/automatic.conf  # the file's layout changed: look
systemctl enable --now dnf-automatic.timer

cat >/usr/local/sbin/reboot-if-needed <<'EOT'
#!/bin/sh
# needs-restarting -r: 0 nothing to do, 1 a reboot is needed, anything else an error.
dnf needs-restarting -r >/dev/null
status=$?
[ "$status" -eq 0 ] && exit 0
[ "$status" -eq 1 ] || exit "$status"
logger -t reboot-if-needed "rebooting for updates"
systemctl reboot
EOT
chmod 755 /usr/local/sbin/reboot-if-needed
cat >/etc/systemd/system/reboot-if-needed.service <<'EOT'
[Unit]
Description=Reboot if an installed update needs it

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/reboot-if-needed
EOT
cat >/etc/systemd/system/reboot-if-needed.timer <<'EOT'
[Unit]
Description=Weekly reboot for updates, only if one is needed

[Timer]
OnCalendar=Sun *-*-* 04:00:00 UTC
Persistent=false

[Install]
WantedBy=timers.target
EOT
systemctl daemon-reload
systemctl enable --now reboot-if-needed.timer
