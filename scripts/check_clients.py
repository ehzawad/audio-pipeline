"""Cross-language protocol verification; no device microphone or carrier is exercised."""
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    subprocess.run(['node', '--test', 'clients/javascript/voice.test.mjs'], cwd=ROOT, check=True)
    subprocess.run(['swift', 'test', '--package-path', 'clients/swift'], cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory() as directory:
        target = Path(directory)
        fixture = json.loads((ROOT/'clients/fixtures/protocol.json').read_text())
        rows = ['\t'.join(['packet', p['hex'], str(p['epoch']), str(p['seq']), ','.join(map(str, p['samples']))]) for p in fixture['packets']]
        rows += ['invalid\t'+hex for hex in fixture['invalid']]
        (target/'fixture.tsv').write_text('\n'.join(rows)+'\n')
        subprocess.run(['kotlinc', 'clients/kotlin/VoiceProtocol.kt', 'clients/kotlin/ProtocolCheck.kt',
                        '-include-runtime', '-d', str(target/'check.jar')], cwd=ROOT, check=True)
        subprocess.run(['java', '-jar', str(target/'check.jar'), str(target/'fixture.tsv')], check=True)


if __name__ == '__main__':
    main()
