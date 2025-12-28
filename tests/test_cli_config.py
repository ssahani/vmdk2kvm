import unittest, tempfile, os
from pathlib import Path

from vmdk2kvm.cli.argument_parser import parse_args_with_config

class TestCLIConfigTwoPhaseParse(unittest.TestCase):
    def test_config_satisfies_required_vmdk(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            vmdk = td / "vm.vmdk"
            vmdk.write_bytes(b"dummy")
            cfg = td / "cfg.yaml"
            cfg.write_text(f"vmdk: {vmdk}\n", encoding="utf-8")

            # This would fail if argparse enforces --vmdk before config defaults are applied.
            args, conf, _logger = parse_args_with_config(argv=["--config", str(cfg), "local"])

            self.assertEqual(Path(args.vmdk), vmdk)
            self.assertIn("vmdk", conf)

if __name__ == "__main__":
    unittest.main()
