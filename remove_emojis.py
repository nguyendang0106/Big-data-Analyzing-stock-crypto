#!/usr/bin/env python3
"""
Script to remove all emojis from markdown files
"""
import re
import glob

def remove_emojis(text):
    """Remove all emojis from text"""
    # Pattern for most emojis
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", flags=re.UNICODE)
    
    # Also remove common emojis manually
    common_emojis = [
        '🎯', '🚀', '📊', '💼', '✅', '❌', '📋', '🏗️', '🛠️', '🔥',
        '📚', '🔐', '🧪', '📈', '🚨', '🎓', '🔄', '📝', '💡', '🔧',
        '📦', '🔬', '📁', '🔍', '📉', '💾', '🌍', '⚠️', '🎊', '🎉',
        '⏰', '✍️', '⚙️', '📄', '💪', '🔒', '🎨', '📱', '💻', '🖥️'
    ]
    
    result = emoji_pattern.sub(r'', text)
    
    # Remove common emojis
    for emoji in common_emojis:
        result = result.replace(emoji, '')
    
    # Clean up multiple spaces
    result = re.sub(r'  +', ' ', result)
    
    # Clean up lines that start with space after emoji removal
    lines = result.split('\n')
    cleaned_lines = [line.lstrip() if line.startswith(' ') and not line.startswith('    ') else line for line in lines]
    result = '\n'.join(cleaned_lines)
    
    return result

def process_markdown_files():
    """Process all .md files in current directory"""
    md_files = glob.glob('*.md')
    
    for md_file in md_files:
        print(f"Processing {md_file}...")
        
        try:
            # Read file
            with open(md_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Remove emojis
            cleaned_content = remove_emojis(content)
            
            # Write back
            with open(md_file, 'w', encoding='utf-8') as f:
                f.write(cleaned_content)
            
            print(f"✓ Cleaned {md_file}")
            
        except Exception as e:
            print(f"✗ Error processing {md_file}: {e}")

if __name__ == '__main__':
    process_markdown_files()
    print("\nDone! All emojis removed from markdown files.")
