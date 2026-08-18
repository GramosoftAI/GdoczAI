// markdown-parser.service.ts
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class MarkdownParserService {

  parse(markdown: string): string {
    if (!markdown) return '';

    let html = markdown;

    // Escape HTML characters first to prevent XSS
    html = this.escapeHtml(html);

    // Parse code blocks (must be done before inline code)
    html = this.parseCodeBlocks(html);

    // Parse headings
    html = this.parseHeadings(html);

    // Parse horizontal rules
    html = html.replace(/^---$/gm, '<hr>');
    html = html.replace(/^\*\*\*$/gm, '<hr>');

    // Parse bold and italic
    html = html.replace(/\*\*\*(.+?)\*\*\*/g, '<strong><em>$1</em></strong>');
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');
    html = html.replace(/___(.+?)___/g, '<strong><em>$1</em></strong>');
    html = html.replace(/__(.+?)__/g, '<strong>$1</strong>');
    html = html.replace(/_(.+?)_/g, '<em>$1</em>');

    // Parse inline code (after bold/italic to avoid conflicts)
    html = html.replace(/`([^`]+)`/g, '<code>$1</code>');

    // Parse links
    html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');

    // Parse images
    html = html.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, '<img src="$2" alt="$1">');

    // Parse lists
    html = this.parseLists(html);

    // Parse tables
    html = this.parseTables(html);

    // Parse blockquotes
    html = this.parseBlockquotes(html);

    // Parse paragraphs
    html = this.parseParagraphs(html);

    return html;
  }

  private escapeHtml(text: string): string {
    const map: { [key: string]: string } = {
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#039;'
    };

    // Don't escape within code blocks (marked with ``` or `)
    const codeBlockRegex = /```[\s\S]*?```|`[^`]*`/g;
    const codeBlocks: string[] = [];

    // Store code blocks temporarily
    text = text.replace(codeBlockRegex, (match) => {
      codeBlocks.push(match);
      return `__CODE_BLOCK_${codeBlocks.length - 1}__`;
    });

    // Escape HTML in non-code parts
    text = text.replace(/[&<>"']/g, (m) => map[m]);

    // Restore code blocks
    codeBlocks.forEach((block, index) => {
      text = text.replace(`__CODE_BLOCK_${index}__`, block);
    });

    return text;
  }

  private parseCodeBlocks(html: string): string {
    // Parse fenced code blocks with language
    html = html.replace(/```(\w+)?\n([\s\S]*?)```/g, (match, lang, code) => {
      const language = lang ? ` class="language-${lang}"` : '';
      return `<pre><code${language}>${code.trim()}</code></pre>`;
    });

    return html;
  }

  private parseHeadings(html: string): string {
    // Parse H1-H6
    html = html.replace(/^######\s+(.+)$/gm, '<h6>$1</h6>');
    html = html.replace(/^#####\s+(.+)$/gm, '<h5>$1</h5>');
    html = html.replace(/^####\s+(.+)$/gm, '<h4>$1</h4>');
    html = html.replace(/^###\s+(.+)$/gm, '<h3>$1</h3>');
    html = html.replace(/^##\s+(.+)$/gm, '<h2>$1</h2>');
    html = html.replace(/^#\s+(.+)$/gm, '<h1>$1</h1>');

    return html;
  }

  private parseLists(html: string): string {
    const lines = html.split('\n');
    const result: string[] = [];
    let inList = false;
    let listType = '';
    let currentIndent = 0;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const unorderedMatch = line.match(/^(\s*)([-*+])\s+(.+)$/);
      const orderedMatch = line.match(/^(\s*)(\d+\.)\s+(.+)$/);

      if (unorderedMatch || orderedMatch) {
        const match = unorderedMatch || orderedMatch;
        const indent = match![1].length;
        const content = match![3];
        const type = unorderedMatch ? 'ul' : 'ol';

        if (!inList) {
          result.push(`<${type}>`);
          inList = true;
          listType = type;
          currentIndent = indent;
        } else if (indent > currentIndent) {
          result.push(`<${type}>`);
          currentIndent = indent;
        } else if (indent < currentIndent) {
          result.push(`</${listType}></li>`);
          currentIndent = indent;
        }

        result.push(`<li>${content}`);
      } else {
        if (inList) {
          result.push(`</li></${listType}>`);
          inList = false;
        }
        result.push(line);
      }
    }

    if (inList) {
      result.push(`</li></${listType}>`);
    }

    return result.join('\n');
  }

  private parseTables(html: string): string {
    const lines = html.split('\n');
    const result: string[] = [];
    let inTable = false;
    let isHeaderRow = false;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();

      // Check if this line is a table row
      if (line.startsWith('|') && line.endsWith('|')) {
        const cells = line.split('|').filter(cell => cell.trim() !== '');

        // Check if next line is separator (---|---|---)
        if (i + 1 < lines.length) {
          const nextLine = lines[i + 1].trim();
          if (nextLine.match(/^\|[\s:-]+\|/)) {
            // This is a header row
            if (!inTable) {
              result.push('<table>');
              result.push('<thead>');
              inTable = true;
            }
            result.push('<tr>');
            cells.forEach(cell => {
              result.push(`<th>${cell.trim()}</th>`);
            });
            result.push('</tr>');
            result.push('</thead>');
            result.push('<tbody>');
            isHeaderRow = true;
            i++; // Skip the separator line
            continue;
          }
        }

        // Regular table row
        if (inTable) {
          result.push('<tr>');
          cells.forEach(cell => {
            result.push(`<td>${cell.trim()}</td>`);
          });
          result.push('</tr>');
        }
      } else {
        if (inTable) {
          result.push('</tbody>');
          result.push('</table>');
          inTable = false;
        }
        result.push(line);
      }
    }

    if (inTable) {
      result.push('</tbody>');
      result.push('</table>');
    }

    return result.join('\n');
  }

  private parseBlockquotes(html: string): string {
    const lines = html.split('\n');
    const result: string[] = [];
    let inBlockquote = false;

    for (const line of lines) {
      if (line.trim().startsWith('>')) {
        const content = line.trim().substring(1).trim();
        if (!inBlockquote) {
          result.push('<blockquote>');
          inBlockquote = true;
        }
        result.push(`<p>${content}</p>`);
      } else {
        if (inBlockquote) {
          result.push('</blockquote>');
          inBlockquote = false;
        }
        result.push(line);
      }
    }

    if (inBlockquote) {
      result.push('</blockquote>');
    }

    return result.join('\n');
  }

  private parseParagraphs(html: string): string {
    // Split by double newlines to identify paragraphs
    const blocks = html.split(/\n\n+/);
    const result: string[] = [];

    for (const block of blocks) {
      const trimmed = block.trim();

      // Skip empty blocks
      if (!trimmed) continue;

      // Don't wrap if already wrapped in HTML tags
      if (trimmed.match(/^<(h[1-6]|pre|table|ul|ol|blockquote|hr)/)) {
        result.push(trimmed);
      } else {
        // Wrap in paragraph tags
        const lines = trimmed.split('\n').filter(line => line.trim());
        if (lines.length > 0) {
          result.push(`<p>${lines.join('<br>')}</p>`);
        }
      }
    }

    return result.join('\n');
  }
}
