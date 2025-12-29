const { HttpsProxyAgent } = require('https-proxy-agent');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

/**
 * 解析自定义代理字符串。
 * @param {string} proxyString - 自定义代理字符串，格式为 'ip:port:账号:密码'。
 * @returns {object|null} 一个结构化的配置对象，如果解析失败则返回 null。
 */
function parseProxyUrl(proxyString) {
    if (!proxyString || typeof proxyString !== 'string' || !proxyString.includes(':')) {
        console.error("错误: 提供的代理字符串无效或格式不正确。");
        return null;
    }

    const parts = proxyString.split(':');
    if (parts.length !== 4) {
        console.error("错误: 代理字符串格式不正确，应为 'ip:port:账号:密码'");
        console.error(`收到的内容: ${proxyString}`);
        return null;
    }

    const [host, port, username, password] = parts;

    return {
        protocol: 'http',
        host: host,
        port: parseInt(port, 10),
        auth: {
            username: username,
            password: password
        }
    };
}

/**
 * 从Excel文件中读取链接和页码
 * @param {string} filePath - Excel文件路径
 * @returns {Promise<Array>} - 返回链接和页码的数组
 */
async function readLinksFromExcel(filePath) {
    const XLSX = require('xlsx');
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(sheet);
    return data.map(row => ({
        link: row.Link,
        pages: row.Pages
    }));
}

/**
 * 提取产品链接
 * @param {string} html - 网页HTML源代码
 * @returns {Array} - 返回提取的产品链接数组
 */
function extractProductLinks(html) {
    const regex = /https:\/\/www\.carrefour\.fr\/p\/[^\s"']+/g;
    return html.match(regex) || [];
}

(async () => {
    const argvUrl = process.argv[2];
    const request = require('./request');

    if (argvUrl) {
        // Called for a single URL (used by Python scraper)
        const pageUrl = argvUrl;
        console.error(`Scraping single URL: ${pageUrl}`);
        try {
            const html = await request.get_page_source(pageUrl, null);
            process.stdout.write(typeof html === 'string' ? html : JSON.stringify(html));
            process.exit(0);
        } catch (err) {
            console.error(`Error fetching page: ${err.message}`);
            process.exit(2);
        }
    }

    // Fallback: process an Excel file from repo root if no arg provided
    const inputFilePath = path.resolve(__dirname, '..', '..', 'input_links.xlsx');
    let linksData = [];
    try {
        linksData = await readLinksFromExcel(inputFilePath);
    } catch (e) {
        console.error(`Failed to read Excel file at ${inputFilePath}: ${e.message}`);
        process.exit(3);
    }

    for (const { link, pages } of linksData) {
        const totalPages = Number(pages) || 1;
        for (let page = 1; page <= totalPages; page++) {
            const pageUrl = `${link}?noRedirect=1&page=${page}`;
            console.error(`正在抓取: ${pageUrl}`);

            let proxyConfig = null;

            try {
                const html = await request.get_page_source(pageUrl, proxyConfig);
                const productLinks = extractProductLinks(html);

                fs.appendFileSync(path.resolve(__dirname, '..', '..', 'product_links.txt'), productLinks.join('\n') + '\n');
                console.error(`成功提取 ${productLinks.length} 个产品链接`);
            } catch (err) {
                console.error(`抓取失败: ${err.message}`);
            }
        }
    }
})();