import json
import os
import re
import subprocess
import urllib.request
import xml.etree.ElementTree as ET

def main():
    csproj_path = os.getenv('CSPROJ_PATH', 'ARSoft.Tools.Net/ARSoft.Tools.Net.csproj')
    explicit_version = os.getenv('EXPLICIT_VERSION', '').strip()
    ref_name = os.getenv('REF_NAME', '')
    ref_type = os.getenv('REF_TYPE', '')

    tree = ET.parse(csproj_path)
    root = tree.getroot()

    pkg_id_elem = root.find('.//PackageId')
    package_id = pkg_id_elem.text.strip() if pkg_id_elem is not None else 'perdub.ARSoft.Tools.Net.DoQ'

    ver_prefix_elem = root.find('.//VersionPrefix')
    ver_prefix = ver_prefix_elem.text.strip() if ver_prefix_elem is not None else '1.0.0'
    base = re.sub(r'-\d+$', '', ver_prefix)

    if explicit_version:
        next_version = explicit_version
        print(f"Using explicitly specified version: {next_version}")
    elif ref_type == 'tag' and ref_name.startswith('v'):
        next_version = ref_name.lstrip('v')
        print(f"Using version from git tag: {next_version}")
    else:
        # 1. Fetch published versions from NuGet
        nuget_versions = []
        try:
            url = f"https://api.nuget.org/v3-flatcontainer/{package_id.lower()}/index.json"
            req = urllib.request.Request(url, headers={'User-Agent': 'CI-AutoBump'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    nuget_versions = json.loads(resp.read().decode()).get('versions', [])
            print(f"Fetched {len(nuget_versions)} version(s) from NuGet")
        except Exception as e:
            print(f"Warning: could not fetch versions from NuGet: {e}")

        # 2. Fetch git tags
        git_tags = []
        try:
            tags_out = subprocess.check_output(['git', 'tag', '-l'], text=True)
            git_tags = [t.strip().lstrip('v') for t in tags_out.splitlines() if t.strip()]
            print(f"Fetched {len(git_tags)} tag(s) from git")
        except Exception as e:
            print(f"Warning: could not fetch git tags: {e}")

        all_known = set(nuget_versions + git_tags)
        pattern = re.compile(rf'^{re.escape(base)}(?:-(\d+))?$')

        max_suffix = 0
        found_base = False
        for v in all_known:
            m = pattern.match(v)
            if m:
                found_base = True
                suffix_str = m.group(1)
                if suffix_str is not None:
                    max_suffix = max(max_suffix, int(suffix_str))

        if max_suffix == 0 and found_base:
            next_suffix = 1
        elif max_suffix > 0:
            next_suffix = max_suffix + 1
        else:
            m_cur = re.search(r'-(\d+)$', ver_prefix)
            next_suffix = int(m_cur.group(1)) if m_cur else 1

        next_version = f"{base}-{next_suffix}"
        print(f"Computed next version: {next_version} (base: {base}, previous max suffix: {max_suffix})")

    # Update csproj in place
    with open(csproj_path, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content = re.sub(r'<VersionPrefix>[^<]+</VersionPrefix>', f'<VersionPrefix>{next_version}</VersionPrefix>', content)
    with open(csproj_path, 'w', encoding='utf-8') as f:
        f.write(new_content)

    print(f"Updated {csproj_path} with VersionPrefix={next_version}")

    github_output = os.getenv('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a', encoding='utf-8') as f:
            f.write(f"version={next_version}\n")

if __name__ == '__main__':
    main()
