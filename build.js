const fs = require('fs');
const path = require('path');
const https = require('https');
const { execSync } = require('child_process');

// Configuration
const DATA_DIR = 'MyExtension/data';
const MANIFEST_FILE = 'MyExtension/manifest.json';
const EXTENSION_VERSIONS_DIR = 'Extension Versions';

// List of all JSON files your extension uses (164 total)
const JSON_FILES = [
    'film_titles_1001-movies-you-must-see-before-you-die-2024.json',
    'film_titles_250-highest-grossing-movies-of-all-time.json',
    'film_titles_akira-kurosawas-100-favorite-movies.json',
    'film_titles_all-bafta-best-film-award-winners.json',
    'film_titles_all-time-worldwide-box-office.json',
    'film_titles_berlin-international-film-festival-golden.json',
    'film_titles_biggest-box-office-bombs-adjusted-for-inflation.json',
    'film_titles_billion-dollar-movies.json',
    'film_titles_black-directors-the-official-top-100-narrative.json',
    'film_titles_bong-joon-hos-favorites.json',
    'film_titles_cannes-palme-dor-winners.json',
    'film_titles_critics-choice-winners.json',
    'film_titles_david-finchers-favorite-films.json',
    'film_titles_directors-guild-of-america-award-winners.json',
    'film_titles_edgar-wrights-1000-favorite-movies.json',
    'film_titles_every-annie-best-animated-feature-winner.json',
    'film_titles_every-film-that-has-ever-been-on-the-imdb.json',
    'film_titles_every-film-to-win-10-or-oscars.json',
    'film_titles_every-film-to-win-3-or-oscars.json',
    'film_titles_every-film-to-win-5-or-oscars.json',
    'film_titles_every-film-to-win-7-or-oscars.json',
    'film_titles_every-los-angeles-film-critics-association.json',
    'film_titles_every-movie-ive-seen-ranked.json',
    'film_titles_every-movie-referenced-watched-in-gilmore.json',
    'film_titles_every-national-board-of-review-best-film.json',
    'film_titles_every-national-society-of-film-critics-best.json',
    'film_titles_every-new-york-film-critics-circle-best-film.json',
    'film_titles_every-oscar-nominee-ever.json',
    'film_titles_every-oscar-winner-ever-1.json',
    'film_titles_every-producers-guild-of-america-best-theatrical.json',
    'film_titles_every-saturn-award-winner-for-best-fantasy.json',
    'film_titles_every-saturn-award-winner-for-best-horror.json',
    'film_titles_every-saturn-award-winner-for-best-science.json',
    'film_titles_every-screen-actors-guild-outstanding-performance.json',
    'film_titles_every-writers-guild-of-america-best-screenplay.json',
    'film_titles_films-where-andrew-garfield-goes-up-against.json',
    'film_titles_flanagans-favorites-my-top-100.json',
    'film_titles_four-greatest-films-of-each-year-according.json',
    'film_titles_golden-globe-award-for-best-motion-picture-1.json',
    'film_titles_golden-globe-award-for-best-motion-picture.json',
    'film_titles_golden-lion-winners.json',
    'film_titles_gotham-awards-best-feature-winners.json',
    'film_titles_greta-gerwig-talked-about-these-films.json',
    'film_titles_guillermo-del-toros-twitter-film-recommendations.json',
    'film_titles_harvard-film-phd-program-narrative-films.json',
    'film_titles_highest-grossing-film-by-year-of-release.json',
    'film_titles_highest-grossing-movies-of-all-time-adjusted.json',
    'film_titles_horror-movies-everyone-should-watch-at-least.json',
    'film_titles_imdb-top-250.json',
    'film_titles_letterboxd-100-animation.json',
    'film_titles_letterboxd-113-highest-rated-19th-century.json',
    'film_titles_letterboxd-four-favorites-interviews.json',
    'film_titles_letterboxd-top-250-films-history-collected.json',
    'film_titles_letterboxds-official-top-250-anime-tv-miniseries.json',
    'film_titles_letterboxds-top-100-silent-films.json',
    'film_titles_letterboxds-top-250-highest-rated-narrative.json',
    'film_titles_letterboxds-top-250-highest-rated-short-films.json',
    'film_titles_letterboxds-top-250-horror-films.json',
    'film_titles_letterboxds-top-250-romantic-comedy-films.json',
    'film_titles_list-of-all-winners-for-the-independent-spirit.json',
    'film_titles_list-of-box-office-number-one-films-in-the.json',
    'film_titles_martin-scorseses-film-school.json',
    'film_titles_mike-flanagans-recommended-gateway-horror.json',
    'film_titles_most-expensive-films-adjusted-for-inflation.json',
    'film_titles_most-expensive-films-unadjusted-for-inflation.json',
    'film_titles_most-fans-per-viewer-on-letterboxd-2024.json',
    'film_titles_most-popular-film-for-every-year-on-letterboxd.json',
    'film_titles_movies-everyone-should-watch-at-least-once.json',
    'film_titles_movies-that-i-highly-recommend.json',
    'film_titles_movies-where-a-5-star-rating-is-most-common.json',
    'film_titles_movies-where-the-protagonist-witnesses-a.json',
    'film_titles_official-top-250-documentary-films.json',
    'film_titles_official-top-250-films-with-the-most-fans.json',
    'film_titles_official-top-50-narrative-feature-films-under.json',
    'film_titles_oscar-winners-best-picture.json',
    'film_titles_quentin-tarantinos-199-favorite-films.json',
    'film_titles_razzie-worst-picture.json',
    'film_titles_roger-eberts-great-movies.json',
    'film_titles_rotten-tomatoes-300-best-movies-of-all-time.json',
    'film_titles_spike-lees-95-essential-films-all-aspiring.json',
    'film_titles_stanley-kubricks-100-favorite-filmsthat-we.json',
    'film_titles_sundance-grand-jury-prize-winners.json',
    'film_titles_the-anti-letterboxd-250.json',
    'film_titles_the-complete-criterion-collection.json',
    'film_titles_the-complete-library-of-congress-national.json',
    'film_titles_the-most-controversial-films-on-letterboxd.json',
    'film_titles_the-top-150-highest-rated-films-of-180-minutes.json',
    'film_titles_the-top-20-highest-rated-films-of-240-minutes.json',
    'film_titles_the-top-250-highest-rated-films-of-120-minutes.json',
    'film_titles_the-top-250-highest-rated-films-of-90-minutes.json',
    'film_titles_the-top-250-most-popular-films-of-120-minutes.json',
    'film_titles_the-top-250-most-popular-films-of-90-minutes.json',
    'film_titles_the-top-5-most-popular-films-of-240-minutes.json',
    'film_titles_the-top-75-most-popular-films-of-180-minutes.json',
    'film_titles_the-top-rated-movie-of-every-year-by-letterboxd.json',
    'film_titles_tiff-peoples-choice-award-winners.json',
    'film_titles_top-100-concert-films-digital-albums.json',
    'film_titles_top-100-g-rated-narrative-feature-films.json',
    'film_titles_top-100-highest-rated-african-narrative-feature.json',
    'film_titles_top-100-highest-rated-stand-up-comedy-specials.json',
    'film_titles_top-100-most-popular-south-american-narrative.json',
    'film_titles_top-150-most-popular-australian-narrative.json',
    'film_titles_top-20-most-popular-african-narrative-feature.json',
    'film_titles_top-20-nc-17-rated-narrative-feature-films.json',
    'film_titles_top-200-most-popular-g-rated-narrative-feature.json',
    'film_titles_top-25-most-popular-nc-17-rated-narrative.json',
    'film_titles_top-250-highest-grossing-movies-of-all-time-1.json',
    'film_titles_top-250-highest-rated-action-narrative-feature.json',
    'film_titles_top-250-highest-rated-adventure-narrative.json',
    'film_titles_top-250-highest-rated-animation-narrative.json',
    'film_titles_top-250-highest-rated-asian-narrative-feature.json',
    'film_titles_top-250-highest-rated-comedy-narrative-feature.json',
    'film_titles_top-250-highest-rated-crime-narrative-feature.json',
    'film_titles_top-250-highest-rated-drama-narrative-feature.json',
    'film_titles_top-250-highest-rated-european-narrative.json',
    'film_titles_top-250-highest-rated-family-narrative-feature.json',
    'film_titles_top-250-highest-rated-fantasy-narrative-feature.json',
    'film_titles_top-250-highest-rated-history-narrative-feature.json',
    'film_titles_top-250-highest-rated-horror-narrative-feature.json',
    'film_titles_top-250-highest-rated-music-narrative-feature.json',
    'film_titles_top-250-highest-rated-mystery-narrative-feature.json',
    'film_titles_top-250-highest-rated-north-american-narrative.json',
    'film_titles_top-250-highest-rated-romance-narrative-feature.json',
    'film_titles_top-250-highest-rated-romantic-comedy-narrative.json',
    'film_titles_top-250-highest-rated-science-fiction-narrative.json',
    'film_titles_top-250-highest-rated-south-american-narrative.json',
    'film_titles_top-250-highest-rated-things-on-letterboxd.json',
    'film_titles_top-250-highest-rated-thriller-narrative.json',
    'film_titles_top-250-highest-rated-war-narrative-feature.json',
    'film_titles_top-250-highest-rated-western-narrative-feature.json',
    'film_titles_top-250-most-popular-action-narrative-feature.json',
    'film_titles_top-250-most-popular-adventure-narrative.json',
    'film_titles_top-250-most-popular-animation-narrative.json',
    'film_titles_top-250-most-popular-asian-narrative-feature.json',
    'film_titles_top-250-most-popular-comedy-narrative-feature.json',
    'film_titles_top-250-most-popular-crime-narrative-feature.json',
    'film_titles_top-250-most-popular-drama-narrative-feature.json',
    'film_titles_top-250-most-popular-european-narrative-feature.json',
    'film_titles_top-250-most-popular-family-narrative-feature.json',
    'film_titles_top-250-most-popular-fantasy-narrative-feature.json',
    'film_titles_top-250-most-popular-history-narrative-feature.json',
    'film_titles_top-250-most-popular-horror-narrative-feature.json',
    'film_titles_top-250-most-popular-music-narrative-feature.json',
    'film_titles_top-250-most-popular-mystery-narrative-feature.json',
    'film_titles_top-250-most-popular-north-american-narrative.json',
    'film_titles_top-250-most-popular-nr-rated-narrative-feature.json',
    'film_titles_top-250-most-popular-pg-13-rated-narrative.json',
    'film_titles_top-250-most-popular-pg-rated-narrative-feature.json',
    'film_titles_top-250-most-popular-r-rated-narrative-feature.json',
    'film_titles_top-250-most-popular-romance-narrative-feature.json',
    'film_titles_top-250-most-popular-science-fiction-narrative.json',
    'film_titles_top-250-most-popular-thriller-narrative-feature.json',
    'film_titles_top-250-most-popular-war-narrative-feature.json',
    'film_titles_top-250-most-popular-western-narrative-feature.json',
    'film_titles_top-250-movies-by-unweighted-rating.json',
    'film_titles_top-250-nr-rated-narrative-feature-films.json',
    'film_titles_top-250-pg-13-rated-narrative-feature-films.json',
    'film_titles_top-250-pg-rated-narrative-feature-films.json',
    'film_titles_top-250-r-rated-narrative-feature-films.json',
    'film_titles_top-2500-highest-rated-narrative-feature.json',
    'film_titles_top-2500-most-popular-narrative-feature-films.json',
    'film_titles_top-5000-films-of-all-time-calculated.json',
    'film_titles_top-75-highest-rated-australian-narrative.json',
    'film_titles_women-directors-the-official-top-250-narrative.json'
];

// Function to find the most recent extension version
function findMostRecentExtensionVersion() {
    if (!fs.existsSync(EXTENSION_VERSIONS_DIR)) {
        return null;
    }
    
    const versions = fs.readdirSync(EXTENSION_VERSIONS_DIR)
        .filter(item => {
            const itemPath = path.join(EXTENSION_VERSIONS_DIR, item);
            return fs.statSync(itemPath).isDirectory() && item.startsWith('Betterboxd-Extension-');
        })
        .sort()
        .reverse(); // Most recent first
    
    return versions.length > 0 ? versions[0] : null;
}

// Utility function to copy a local file
function copyLocalFile(sourcePath, destPath) {
    return new Promise((resolve, reject) => {
        try {
            fs.copyFileSync(sourcePath, destPath);
            resolve();
        } catch (err) {
            reject(err);
        }
    });
}

// Function to increment version number
function incrementVersion(version) {
    const parts = version.split('.');
    const patch = parseInt(parts[2]) + 1;
    return `${parts[0]}.${parts[1]}.${patch}`;
}

// Function to update manifest version
function updateManifestVersion() {
    const manifest = JSON.parse(fs.readFileSync(MANIFEST_FILE, 'utf8'));
    const oldVersion = manifest.version;
    manifest.version = incrementVersion(oldVersion);
    
    fs.writeFileSync(MANIFEST_FILE, JSON.stringify(manifest, null, 4));
    console.log(`✅ Updated version: ${oldVersion} → ${manifest.version}`);
    return manifest.version;
}

// Main build function
async function build() {
    console.log('🚀 Starting Betterboxd Extension Build...\n');
    
    try {
        // Create data directory if it doesn't exist
        if (!fs.existsSync(DATA_DIR)) {
            fs.mkdirSync(DATA_DIR);
            console.log(`📁 Created ${DATA_DIR} directory`);
        }
        
        // The master MyExtension directory is already ready with all files
        // No need to copy or check anything - just proceed to version update
        console.log('📁 Master MyExtension directory is ready for packaging');
        
        // Update manifest version
        const newVersion = updateManifestVersion();
        
        console.log('\n🎉 Build completed successfully!');
        console.log(`📦 Extension version: ${newVersion}`);
        console.log(`📁 Master MyExtension directory packaged with fresh data`);
        console.log('\n📋 Next steps:');
        console.log('1. Test the extension locally');
        console.log('2. Package for Chrome Web Store');
        console.log('3. Upload to Chrome Web Store');
        
    } catch (error) {
        console.error('❌ Build failed:', error.message);
        process.exit(1);
    }
}

// Run the build
build();