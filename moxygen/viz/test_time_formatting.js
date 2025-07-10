// Quick test to validate time formatting methods
// This shows the difference between formatTime and formatTimeInSeconds

// Simulate the two formatting methods
function formatTime(timeMs) {
    if (timeMs < 1000) {
        return `${timeMs.toFixed(0)}ms`;
    } else if (timeMs < 60000) {
        return `${(timeMs / 1000).toFixed(2)}s`;
    } else {
        const minutes = Math.floor(timeMs / 60000);
        const seconds = ((timeMs % 60000) / 1000).toFixed(1);
        return `${minutes}:${seconds.padStart(4, '0')}`;
    }
}

function formatTimeInSeconds(timeMs) {
    if (timeMs < 60000) {
        return `${(timeMs / 1000).toFixed(3)}s`;
    } else {
        const minutes = Math.floor(timeMs / 60000);
        const seconds = ((timeMs % 60000) / 1000).toFixed(2);
        return `${minutes}:${seconds.padStart(5, '0')}`;
    }
}

// Test cases from the sample data
const testTimes = [20, 40, 480, 500, 1500, 9500];

console.log("Time Formatting Comparison:");
console.log("Time (ms) | formatTime (old) | formatTimeInSeconds (new)");
console.log("----------|------------------|----------------------");

testTimes.forEach(time => {
    const oldFormat = formatTime(time);
    const newFormat = formatTimeInSeconds(time);
    console.log(`${time.toString().padStart(9)} | ${oldFormat.padStart(16)} | ${newFormat}`);
});