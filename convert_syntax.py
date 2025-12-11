import os
import re

def convert_admonitions(content):
    # MkDocs: !!! note "Title" -> Docusaurus: :::note[Title]
    # We process line by line to handle closure and identation.

    # Handle indentations (MkDocs requires indentation, Docusaurus doesn't need it but tolerates it)
    # However, to be clean, we should probably unindent the content block, but that's hard.
    # For now, just converting the fence is usually enough if it's top level.
    # But MkDocs uses indentation to end the block. Docusaurus uses ::: to end.
    # We need to find the end of the block. This is tricky with regex.
    # A simple heuristc: MkDocs blocks usually end with a newline and unindented text, or empty lines.
    # For now, let's just replace the start syntax. Docusaurus usually requires an ending ":::".
    # Since we can't reliably find the end, we might need a parser usage.
    # ALTERNATIVE: Use Docusaurus "remark-admonitions" plugin compatibility?
    # Actually, Docusaurus supports ':::' blocks. MkDocs uses indentation.
    # We will try to scan line by line.
    
    lines = content.split('\n')
    new_lines = []
    
    # State tracking
    in_admonition = False
    admonition_indent = 0
    
    in_tab = False
    tab_indent = 0
    
    for i, line in enumerate(lines):
        # 1. Check for Admonition Start
        match = re.search(r'^(\s*)!!!\s+(\w+)(?:\s+"(.*?)")?', line)
        if match:
            new_indent = len(match.group(1))
            
            # Close previous block if needed
            if in_admonition:
                if new_indent <= admonition_indent:
                    new_lines.append(f"{' ' * admonition_indent}:::")
                    in_admonition = False
            
            # If line is indented inside a tab, we probably should keep it indented?
            # Or unindent it relative to tab?
            # If we flatten tabs, we unindent EVERYTHING.
            # So if we are in_tab, we expect content to be at tab_indent + 4.
            # But the '!!!' line itself is at tab_indent + 4 (if nested in tab).
            
            # Start new admonition
            indent_str = match.group(1)
            type_ = match.group(2)
            title = match.group(3)
            
            # Logic: If we are flattening tabs, we want to strip `tab_indent + 4` from this line 
            # effectively shifting it left.
            # But `match` captured indentation.
            
            in_admonition = True
            admonition_indent = new_indent
            
            # If we are in a tab, we likely want to adjust `admonition_indent`?
            # Actually, simply outputting the converted `:::` line with its current indentation 
            # (which might be indented if my tab logic is naive) is tricky.
            
            # Let's simplify: if we see `===`, we set `in_tab` and `tab_indent`.
            # Any subsequent line that is indented > `tab_indent` gets unindented by 4 spaces.
            
            if title:
                new_lines.append(f"{indent_str}:::{type_}[{title}]")
            else:
                new_lines.append(f"{indent_str}:::{type_}")
            continue
            
        # 2. Check for Tab Start (=== "Title")
        match_tab = re.search(r'^(\s*)===\s*"(.*?)"', line)
        if match_tab:
            new_indent = len(match_tab.group(1))
            
            # If we were in an admonition, check if this tab breaks it?
            if in_admonition:
                 if new_indent <= admonition_indent:
                      new_lines.append(f"{' ' * admonition_indent}:::")
                      in_admonition = False
            
            # Update Tab State
            # We treat tabs as blocks that just need unindenting.
            # If we are already in a tab at same level, we just continue (switch tab).
            # If nested tab? ignoring nested tabs for now complexity.
            
            in_tab = True
            tab_indent = new_indent
            
            title = match_tab.group(2)
            indent_str = match_tab.group(1)
            
            new_lines.append(f"{indent_str}**{title}**")
            continue

        stripped = line.strip()
        
        # 3. Handle End of Admonition logic
        if in_admonition:
            if not stripped:
                new_lines.append(line)
                continue
            
            current_indent = len(line) - len(line.lstrip())
            if current_indent <= admonition_indent:
                new_lines.append(f"{' ' * admonition_indent}:::")
                in_admonition = False
                # Fallthrough to process as normal line (or tab content)
            else:
                # Inside Admonition: Unindent
                base_indent = admonition_indent + 4
                if len(line) >= base_indent and line[:base_indent].strip() == "":
                    new_line = line[base_indent:]
                else:
                    if len(line) > admonition_indent:
                        new_line = line[admonition_indent:].lstrip()
                    else:
                        new_line = line
                new_lines.append(new_line)
                continue

        # 4. Handle End of Tab logic (if not in admonition)
        if in_tab:
            if not stripped:
                new_lines.append(line)
                continue
            
            current_indent = len(line) - len(line.lstrip())
            
            # If indent matches tab_indent or less -> Tab ended (probably)
            # MkDocs tabs content must be indented.
            if current_indent <= tab_indent:
                in_tab = False
                # Fallthrough to normal output
            else:
                # Inside Tab: Unindent 4 spaces
                # We assume standard 4 space indent for tab content
                base_indent = tab_indent + 4
                if len(line) >= base_indent and line[:base_indent].strip() == "":
                    new_line = line[base_indent:]
                else:
                     # Fallback
                     new_line = line
                new_lines.append(new_line)
                continue

        new_lines.append(line)
        
    if in_admonition:
        new_lines.append(f"{' ' * admonition_indent}:::")
        
    return '\n'.join(new_lines)


def strip_mkdocs_attributes(content):
    # MkDocs uses { .annotate } or {: .class }
    # MDX parses {} as JS. We need to escape them or remove them.
    # For { .annotate }, it's usually on its own line.
    
    # Remove { .annotate } lines
    content = re.sub(r'^\s*\{\s*\.annotate\s*\}\s*$', '', content, flags=re.MULTILINE)
    
    # Remove {: .class } or { #id }
    # This is risky if it matches valid JS, but usually MkDocs attributes are specific.
    # For now, let's just target the known failure: { .annotate }
    return content

def strip_html_styles(content):
    # Remove style="..." from HTML tags
    # This is a crude regex but safe for simple cases found in these docs
    content = re.sub(r'\s+style="[^"]*"', '', content)
    return content

def convert_tabs(content):
    # MkDocs: === "Tab Name"
    # Docusaurus: <TabItem value="Tab Name"> and wrapping <Tabs>
    # This is quite complex to regex.
    # We will do a simple pass:
    # If we see `=== "..."`, we start a Tabs block if not started.
    
    lines = content.split('\n')
    new_lines = []
    in_tabs = False
    
    # We need to import Tabs/TabItem if we use them
    has_tabs = '=== "' in content
    
    for i, line in enumerate(lines):
        match = re.match(r'^(\s*)===\s*"(.*?)"', line)
        if match:
            indent = match.group(1)
            label = match.group(2)
            
            if not in_tabs:
                new_lines.append(f"{indent}<Tabs>")
                in_tabs = True
            
            # Close previous tab item if we were in one?
            # Actually, pymdown tabs are sequential.
            # Docusaurus <TabItem> wraps content.
            # It's hard to convert indentation-based whitespace blocks to XML tags without strict parsing.
            # For this task, strictly automating tabs might break things.
            # Let's SKIP tabs automated conversion for now and let the user review,
            # OR just do a comment annotation.
            pass
        else:
             if in_tabs and line.strip() == "":
                 # potential end of tabs?
                 pass

    # Basic Replace for now just to make it compile-safe (maybe?)
    # Actually, let's leave tabs alone or use a simpler replacement if possible.
    # Retaining `===` will just render as text, which is safe.
    return content

def main():
    root_dir = "v1/website/docs"
    
    # Prepend imports if needed
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".md"):
                path = os.path.join(root, file)
                with open(path, "r") as f:
                    content = f.read()
                
                new_content = convert_admonitions(content)
                new_content = strip_mkdocs_attributes(new_content)
                new_content = strip_html_styles(new_content)
                
                # Check if we need to add Tabs imports
                if '<Tabs>' in new_content and 'import Tabs' not in new_content:
                     new_content = "import Tabs from '@theme/Tabs';\nimport TabItem from '@theme/TabItem';\n\n" + new_content
                
                if new_content != content:
                    with open(path, "w") as f:
                        f.write(new_content)
                    print(f"Converted {path}")

if __name__ == "__main__":
    main()
