const http = require('http');
const app = require('./server.js');

const PORT = 5097;
const server = http.createServer(app);

server.listen(PORT, async () => {
    try {
        console.log(`[Test Suite] Running API tests on http://localhost:${PORT}...`);

        // 1. Health check
        const healthRes = await fetch(`http://localhost:${PORT}/api/health`);
        const health = await healthRes.json();
        if (healthRes.status !== 200 || health.status !== 'online') {
            throw new Error(`Health check failed: ${healthRes.status}`);
        }
        console.log('✔ Health endpoint OK (status: online)');

        // 2. Site Data
        const siteRes = await fetch(`http://localhost:${PORT}/api/site-data`);
        const siteData = await siteRes.json();
        if (
            siteRes.status !== 200 ||
            !Array.isArray(siteData.djPackages) ||
            siteData.djPackages.length !== 5 ||
            !Array.isArray(siteData.portfolioCategories) ||
            siteData.portfolioCategories.length !== 7 ||
            siteData.signatureServices.length !== 6 ||
            siteData.ragaStudioServices.length !== 8 ||
            siteData.academyCourses.length !== 4
        ) {
            throw new Error(`Site data verification failed`);
        }
        console.log('✔ Site data endpoint OK (5 DJ packages, 7 categories, 6 signature, 8 studio, 4 academy)');

        // 3. Admin Auth Status
        const authRes = await fetch(`http://localhost:${PORT}/api/admin/auth/status`);
        const authData = await authRes.json();
        if (authRes.status !== 200 || !authData.ownerEmailHint) {
            throw new Error(`Admin auth status failed`);
        }
        console.log('✔ Admin auth status OK (ownerEmailHint: ' + authData.ownerEmailHint + ')');

        // 4. Contact Enquiry Submission
        const contactRes = await fetch(`http://localhost:${PORT}/api/contact`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                fullName: 'Verification Test Lead',
                email: 'testlead@example.com',
                phone: '9965368899',
                category: 'SS Audios – Event & Entertainment',
                service: 'Goldstar Orchestra (₹25,000)',
                date: '2026-12-25',
                time: '19:00',
                venue: 'Erode Texvalley Hall',
                budget: '₹40,000'
            })
        });
        const contactData = await contactRes.json();
        if (contactRes.status !== 200 || !contactData.success || !contactData.inquiryId) {
            throw new Error('Contact booking submission failed');
        }
        console.log('✔ Contact booking submission OK (inquiryId: ' + contactData.inquiryId + ')');

        // 5. Inquiries list
        const inqRes = await fetch(`http://localhost:${PORT}/api/inquiries`);
        const inqData = await inqRes.json();
        if (inqRes.status !== 200 || !inqData.success || !Array.isArray(inqData.data)) {
            throw new Error('Inquiries listing failed');
        }
        console.log(`✔ Inquiries list OK (${inqData.count} inquiries recorded)`);

        console.log('\n🎉 ALL SS-AUDIOS-BACKEND API TESTS PASSED!');
        server.close(() => process.exit(0));
    } catch (err) {
        console.error('❌ Test failed:', err.message);
        server.close(() => process.exit(1));
    }
});
